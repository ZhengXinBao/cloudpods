package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"yunion.io/x/cloudmux/pkg/multicloud/esxi"
	"yunion.io/x/log"
	"yunion.io/x/pkg/util/httputils"
	"yunion.io/x/pkg/util/stringutils"
	"yunion.io/x/sqlchemy"

	"yunion.io/x/onecloud/pkg/apis"
	api "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/apis/identity"
	"yunion.io/x/onecloud/pkg/cloudcommon"
	common_app "yunion.io/x/onecloud/pkg/cloudcommon/app"
	"yunion.io/x/onecloud/pkg/cloudcommon/db"
	"yunion.io/x/onecloud/pkg/cloudcommon/db/taskman"
	common_options "yunion.io/x/onecloud/pkg/cloudcommon/options"
	"yunion.io/x/onecloud/pkg/compute/models"
	"yunion.io/x/onecloud/pkg/compute/options"
	"yunion.io/x/onecloud/pkg/compute/policy"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
	"yunion.io/x/onecloud/pkg/compute/syncworker"
	"yunion.io/x/onecloud/pkg/mcclient/auth"
)

// StartSyncWorker shares model/driver registration with region but does not
// start its master cron jobs, run InitializeData migrations, or serve its API.
func StartSyncWorker() error {
	opts := &options.Options
	common_options.ParseOptions(opts, os.Args, "region.conf", api.SERVICE_TYPE)
	dialect, dsn, err := opts.DBOptions.GetDBConnection()
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	if dialect != "mysql" {
		return fmt.Errorf("independent sync requires MySQL")
	}
	if opts.SyncWorkerMigrate || opts.SyncWorkerInspectAccount != "" {
		conn, err := sql.Open(dialect, dsn)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		queue := syncqueue.New(conn)
		if opts.SyncWorkerMigrate {
			return queue.Migrate(ctx)
		}
		if err := queue.CheckSchema(ctx); err != nil {
			return err
		}
		stats, err := queue.Stats(ctx, opts.SyncWorkerInspectAccount)
		if err != nil {
			return err
		}
		jobs, err := queue.List(ctx, opts.SyncWorkerInspectAccount, 100)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"counts": stats, "jobs": jobs})
	}
	if opts.LockmanMethod != common_options.LockMethodEtcd {
		return fmt.Errorf("independent workers require lockman_method=etcd on both region and workers")
	}
	config := syncworker.Config{
		WorkerID: stringutils.UUID4(), Concurrency: opts.SyncWorkerConcurrency,
		PollInterval:      time.Duration(opts.SyncWorkerPollSeconds) * time.Second,
		HeartbeatInterval: time.Duration(opts.SyncWorkerHeartbeatSeconds) * time.Second,
		LeaseDuration:     time.Duration(opts.SyncWorkerLeaseSeconds) * time.Second,
		MaxAttempts:       opts.SyncWorkerMaxAttempts, RetryDelay: time.Duration(opts.SyncWorkerRetrySeconds) * time.Second,
		ShutdownTimeout: time.Duration(opts.SyncWorkerShutdownSeconds) * time.Second,
	}
	if err := config.Validate(); err != nil {
		return err
	}
	policy.Init()
	common_app.InitAuth(&opts.CommonOptions, func() { log.Infof("sync worker authentication ready") })
	if err := initEtcdLockOpts(opts); err != nil {
		return err
	}
	if err := esxi.InitEsxiConfig(opts.EsxiOptions); err != nil {
		return err
	}
	cloudcommon.InitDB(&opts.DBOptions)
	defer cloudcommon.CloseDB()
	// Register managers and callback handlers locally, without listening on a port.
	app := common_app.InitApp(&opts.BaseOptions, true)
	InitHandlers(app, true)
	configureSyncWorkerChecksums(db.GlobalModelManagerTables(), opts.EnableDBChecksumTables)
	conn := sqlchemy.GetDB()
	conn.SetMaxOpenConns(config.Concurrency*4 + 8)
	conn.SetMaxIdleConns(config.Concurrency*2 + 4)
	queue := models.CloudSyncQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = queue.CheckSchema(ctx)
	cancel()
	if err != nil {
		return err
	}
	serviceURL, err := auth.GetServiceURL(apis.SERVICE_TYPE_REGION, opts.Region, "", identity.EndpointInterfaceInternal, httputils.POST)
	if err != nil {
		return err
	}
	taskman.SetServiceUrl(serviceURL)
	tlsConfig, err := opts.DBOptions.GetEtcdTLSConfig()
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: opts.EtcdEndpoints, Username: opts.EtcdUsername, Password: opts.EtcdPassword, TLS: tlsConfig, DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer cli.Close()
	fatal := func(err error) {
		log.Errorf("sync worker stopping to protect execution ownership: %v", err)
		os.Exit(1)
	}
	locker, err := syncworker.NewEtcdLocker(cli, opts.EtcdLockPrefix+"/independent-cloud-sync", 30, fatal)
	if err != nil {
		return err
	}
	execute := func(ctx context.Context, job *syncqueue.Job) (executionErr error) {
		release, err := locker.Acquire(ctx, job)
		if err != nil {
			return err
		}
		defer release()
		// Claim may have expired while waiting for a previous execution's mutex.
		checkCtx, cancel := context.WithTimeout(ctx, config.HeartbeatInterval)
		err = queue.Heartbeat(checkCtx, job, config.LeaseDuration)
		cancel()
		if err != nil {
			return err
		}
		defer func() {
			if executionErr != nil && (job.ResourceGroup == "" || job.ResourceGroup == "core" || job.ScopeType == "provider") {
				if err := models.ResetIndependentSyncFailure(ctx, auth.AdminCredential(), job); err != nil {
					log.Errorf("reset failed sync display %s: %v", job.ID, err)
				}
			}
		}()
		if job.Attempts > config.MaxAttempts {
			return fmt.Errorf("sync retry budget exhausted after worker recovery")
		}
		log.Infof("sync worker executing job=%s run-account=%s provider=%s region=%s attempt=%d requester=%s", job.ID, job.AccountID, job.ProviderID, job.RegionID, job.Attempts, job.RequestedBy)
		return models.ExecuteCloudSyncJob(ctx, auth.AdminCredential(), job)
	}
	runner, err := syncworker.New(config, queue, execute, fatal)
	if err != nil {
		return err
	}
	runner.OnError = func(err error) { log.Errorf("sync worker: %v", err) }
	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Infof("sync worker %s ready, concurrency=%d", config.WorkerID, config.Concurrency)
	return runner.Run(runCtx)
}
