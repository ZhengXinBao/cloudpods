package syncworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"yunion.io/x/cloudmux/pkg/cloudprovider"
	"yunion.io/x/onecloud/pkg/compute/syncqueue"
)

// EtcdLocker serializes execution across worker processes for a provider or
// provider-region resource.
// Fatal must terminate the process on lease uncertainty. This is operational
// exclusion, not a fencing token enforced by cloud APIs: a suspended process can
// resume after its lease expires. Strict partition-safe side effects would also
// require the downstream operations to reject stale fencing tokens.
type EtcdLocker struct {
	client *clientv3.Client
	prefix string
	ttl    int
	fatal  func(error)
}

func NewEtcdLocker(client *clientv3.Client, prefix string, ttl int, fatal func(error)) (*EtcdLocker, error) {
	if client == nil || strings.Trim(prefix, "/ ") == "" || ttl < 4 || fatal == nil {
		return nil, errors.New("etcd client, lock prefix, TTL >= 4 seconds and fatal callback are required")
	}
	return &EtcdLocker{client: client, prefix: strings.TrimRight(prefix, "/"), ttl: ttl, fatal: fatal}, nil
}

func allRegionExecutionLockGroups() []string {
	return []string{"core", "extended", "services", "storage"}
}

func executionLockGroups(job *syncqueue.Job) []string {
	scope := job.ScopeType
	if scope == "" {
		scope = "region"
	}
	if scope == "provider" {
		group := job.ResourceGroup
		if group == "" || group == "core" {
			group = "provider"
		}
		return []string{group}
	}
	if job.ResourceGroup == "" {
		return allRegionExecutionLockGroups()
	}
	if job.ResourceGroup == "core" {
		return []string{"core"}
	}
	if job.ResourceGroup != "requested" {
		return []string{job.ResourceGroup}
	}

	var rangeData struct {
		Resources []string `json:"resources"`
	}
	if err := json.Unmarshal([]byte(job.RangeJSON), &rangeData); err != nil || len(rangeData.Resources) == 0 {
		// An unparseable requested range is safer when it waits behind both
		// shared write sets than when it runs concurrently with either one.
		return allRegionExecutionLockGroups()
	}
	groups := map[string]bool{}
	unknown := false
	for _, resource := range rangeData.Resources {
		switch resource {
		case cloudprovider.CLOUD_CAPABILITY_COMPUTE,
			cloudprovider.CLOUD_CAPABILITY_NETWORK,
			cloudprovider.CLOUD_CAPABILITY_EIP,
			cloudprovider.CLOUD_CAPABILITY_NAT,
			cloudprovider.CLOUD_CAPABILITY_SNAPSHOT_POLICY,
			cloudprovider.CLOUD_CAPABILITY_QUOTA,
			cloudprovider.CLOUD_CAPABILITY_IMAGE,
			cloudprovider.CLOUD_CAPABILITY_SECURITY_GROUP,
			cloudprovider.CLOUD_CAPABILITY_VPC_PEER,
			cloudprovider.CLOUD_CAPABILITY_IPV6_GATEWAY:
			groups["core"] = true
		case cloudprovider.CLOUD_CAPABILITY_LOADBALANCER,
			cloudprovider.CLOUD_CAPABILITY_RDS,
			cloudprovider.CLOUD_CAPABILITY_CACHE:
			groups["services"] = true
		case cloudprovider.CLOUD_CAPABILITY_OBJECTSTORE,
			cloudprovider.CLOUD_CAPABILITY_NAS:
			groups["storage"] = true
		case cloudprovider.CLOUD_CAPABILITY_WAF,
			cloudprovider.CLOUD_CAPABILITY_MONGO_DB,
			cloudprovider.CLOUD_CAPABILITY_ES,
			cloudprovider.CLOUD_CAPABILITY_KAFKA,
			cloudprovider.CLOUD_CAPABILITY_APP,
			cloudprovider.CLOUD_CAPABILITY_CONTAINER,
			cloudprovider.CLOUD_CAPABILITY_TABLESTORE,
			cloudprovider.CLOUD_CAPABILITY_MODELARTES,
			cloudprovider.CLOUD_CAPABILITY_MISC:
			groups["extended"] = true
		default:
			unknown = true
		}
	}
	if unknown || len(groups) == 0 {
		return allRegionExecutionLockGroups()
	}
	ret := make([]string, 0, len(groups))
	for group := range groups {
		ret = append(ret, group)
	}
	sort.Strings(ret)
	return ret
}

func executionLockKeyForGroup(prefix string, job *syncqueue.Job, group string) string {
	scope := job.ScopeType
	if scope == "" {
		scope = "region"
	}
	identityParts := []string{job.ProviderID}
	switch scope {
	case "provider":
		identityParts = append(identityParts, scope, job.ResourceGroup)
	default:
		identityParts = append(identityParts, job.RegionID)
		// Keep the core key compatible with pre-group queue jobs. Side groups have
		// independent write sets and can run beside the core sync.
		if group != "" && group != "core" {
			identityParts = append(identityParts, scope, group)
		}
	}
	identity, _ := json.Marshal(identityParts)
	digest := sha256.Sum256(identity)
	return strings.TrimRight(prefix, "/") + "/" + hex.EncodeToString(digest[:])
}

func executionLockKeys(prefix string, job *syncqueue.Job) []string {
	groups := executionLockGroups(job)
	keys := make([]string, 0, len(groups))
	for _, group := range groups {
		keys = append(keys, executionLockKeyForGroup(prefix, job, group))
	}
	sort.Strings(keys)
	return keys
}

func executionLockKey(prefix string, job *syncqueue.Job) string {
	return executionLockKeys(prefix, job)[0]
}

// Acquire's context controls waiting only. Once acquired, the session survives
// cancellation until release is called after the legacy operation has returned.
// The caller must revalidate queue ownership after acquisition and before sync.
func (l *EtcdLocker) Acquire(ctx context.Context, job *syncqueue.Job) (func(), error) {
	if job == nil || job.ProviderID == "" {
		return nil, errors.New("execution lock requires a provider")
	}
	scope := job.ScopeType
	if scope == "" {
		scope = "region"
	}
	switch scope {
	case "region":
		if job.RegionID == "" {
			return nil, errors.New("region execution lock requires a region")
		}
	case "provider":
	default:
		return nil, fmt.Errorf("unsupported sync scope %q", job.ScopeType)
	}
	interval := time.Duration(l.ttl) * time.Second / 4
	grantCtx, cancel := context.WithTimeout(ctx, interval)
	grant, err := l.client.Grant(grantCtx, int64(l.ttl))
	cancel()
	if err != nil {
		return nil, fmt.Errorf("grant execution lease: %w", err)
	}
	// Using a pre-granted lease bounds acquisition without binding session lifetime
	// to the job's cancellation context.
	session, err := concurrency.NewSession(l.client, concurrency.WithTTL(l.ttl), concurrency.WithLease(grant.ID))
	if err != nil {
		return nil, fmt.Errorf("start execution lease: %w", err)
	}
	keys := executionLockKeys(l.prefix, job)
	mutexes := make([]*concurrency.Mutex, 0, len(keys))
	for _, key := range keys {
		mutexes = append(mutexes, concurrency.NewMutex(session, key))
	}
	stop := make(chan struct{})
	stopped := make(chan struct{})
	lost := make(chan error, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(stop); <-stopped; _ = session.Close() }) }
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		fail := func(err error) {
			select {
			case <-stop:
				return
			default:
			}
			err = fmt.Errorf("execution lock for job %s lost: %w", job.ID, err)
			lost <- err
			l.fatal(err)
		}
		for {
			select {
			case <-stop:
				return
			case <-session.Done():
				fail(errors.New("etcd session ended"))
				return
			case <-ticker.C:
				probeCtx, probeCancel := context.WithTimeout(l.client.Ctx(), interval)
				result := make(chan error, 1)
				go func() {
					response, err := l.client.KeepAliveOnce(probeCtx, session.Lease())
					if err == nil && (response == nil || response.TTL <= 0) {
						err = errors.New("etcd lease expired")
					}
					result <- err
				}()
				var err error
				select {
				case <-stop:
					probeCancel()
					return
				case <-session.Done():
					err = errors.New("etcd session ended")
				case err = <-result:
				case <-probeCtx.Done():
					err = probeCtx.Err()
				}
				probeCancel()
				if err != nil {
					fail(err)
					return
				}
			}
		}
	}()
	for _, mutex := range mutexes {
		for {
			if err := ctx.Err(); err != nil {
				release()
				return nil, err
			}
			select {
			case err := <-lost:
				release()
				return nil, err
			default:
			}
			attemptCtx, attemptCancel := context.WithTimeout(ctx, interval)
			err = mutex.TryLock(attemptCtx)
			attemptCancel()
			if err == nil {
				select {
				case err := <-lost:
					release()
					return nil, err
				case <-session.Done():
					release()
					return nil, errors.New("execution session ended during acquisition")
				default:
				}
				break
			}
			if !errors.Is(err, concurrency.ErrLocked) {
				release()
				return nil, fmt.Errorf("acquire execution lock: %w", err)
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				release()
				return nil, ctx.Err()
			case err := <-lost:
				timer.Stop()
				release()
				return nil, err
			case <-timer.C:
			}
		}
	}
	return release, nil
}
