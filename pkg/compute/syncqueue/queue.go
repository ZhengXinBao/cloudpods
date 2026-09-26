// Package syncqueue provides a durable MySQL queue. Call Migrate explicitly before
// starting producers or workers. Connections must use parseTime=true and UTC.
package syncqueue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrLeaseLost = errors.New("sync queue lease lost")

const SchemaVersion = 2
const (
	Waiting   = "waiting"
	Running   = "running"
	Retry     = "retry"
	Succeeded = "succeeded"
	Failed    = "failed"
)

type Request struct {
	RunID         string `json:"run_id,omitempty"`
	AccountID     string `json:"account_id"`
	ProviderID    string `json:"provider_id"`
	RegionID      string `json:"region_id"`
	ScopeType     string `json:"scope_type,omitempty"`
	ResourceGroup string `json:"resource_group,omitempty"`
	RangeJSON     string `json:"range_json"`
	RequestedBy   string `json:"requested_by"`
	ProjectID     string `json:"project_id"`
	DomainID      string `json:"domain_id"`
}
type Job struct {
	Request
	ID          string       `json:"id"`
	ResourceKey string       `json:"resource_key"`
	State       string       `json:"state"`
	WorkerID    string       `json:"worker_id"`
	Version     int64        `json:"version"`
	Attempts    int          `json:"attempts"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	AvailableAt time.Time    `json:"available_at"`
	LeaseUntil  sql.NullTime `json:"lease_until"`
	LastError   string       `json:"last_error"`
	ErrorClass  ErrorClass   `json:"error_class"`
}

// Limits count live running leases across all replicas. Configure every worker
// identically; values <= 0 use conservative defaults 16/8/2.
type Queue struct {
	DB                                       *sql.DB
	GlobalLimit, AccountLimit, ProviderLimit int
}

func (q *Queue) limits() (int, int, int) {
	g, a, p := q.GlobalLimit, q.AccountLimit, q.ProviderLimit
	if g <= 0 {
		g = 16
	}
	if a <= 0 {
		a = 8
	}
	if p <= 0 {
		p = 2
	}
	return g, a, p
}

func New(db *sql.DB) *Queue { return &Queue{DB: db} }

func identity(r Request) (string, string, error) {
	for _, s := range []string{r.AccountID, r.ProviderID} {
		if strings.TrimSpace(s) == "" || len(s) > 128 {
			return "", "", errors.New("account and provider IDs must be 1..128 bytes")
		}
	}
	scope := r.ScopeType
	if scope == "" {
		scope = "region"
	}
	switch scope {
	case "region":
		if strings.TrimSpace(r.RegionID) == "" || len(r.RegionID) > 128 {
			return "", "", errors.New("region scope requires a region ID of 1..128 bytes")
		}
	case "provider":
		// Provider jobs deliberately have no region identity.
	default:
		return "", "", fmt.Errorf("unsupported sync scope %q", r.ScopeType)
	}
	for _, s := range []string{r.RequestedBy, r.ProjectID, r.DomainID, r.RunID, r.ScopeType, r.ResourceGroup} {
		if len(s) > 128 {
			return "", "", errors.New("identity exceeds 128 bytes")
		}
	}
	if len(r.RangeJSON) > 1024*1024 {
		return "", "", errors.New("sync range exceeds 1 MiB")
	}
	var value interface{}
	decoder := json.NewDecoder(strings.NewReader(r.RangeJSON))
	decoder.UseNumber()
	if !json.Valid([]byte(r.RangeJSON)) {
		return "", "", errors.New("invalid sync range JSON")
	}
	if err := decoder.Decode(&value); err != nil {
		return "", "", err
	}
	canonical, _ := json.Marshal(value)
	// JSON tuple framing prevents delimiter collisions in IDs.
	resourceParts := []string{r.ProviderID}
	if scope == "provider" {
		resourceParts = append(resourceParts, r.ScopeType, r.ResourceGroup)
	} else {
		resourceParts = append(resourceParts, r.RegionID)
		if r.ScopeType != "" || r.ResourceGroup != "" {
			resourceParts = append(resourceParts, r.ScopeType, r.ResourceGroup)
		}
	}
	resource, _ := json.Marshal(resourceParts)
	sum := sha256.Sum256(resource)
	key := hex.EncodeToString(sum[:])
	sum = sha256.Sum256(append([]byte(key), canonical...))
	return key, hex.EncodeToString(sum[:]), nil
}

// Migrate performs only additive DDL. It must be an explicit operator action.
func (q *Queue) Migrate(ctx context.Context) error {
	_, err := q.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sync_queue_schema (id INT PRIMARY KEY, version INT NOT NULL) ENGINE=InnoDB`)
	if err != nil {
		return err
	}
	version := 0
	err = q.DB.QueryRowContext(ctx, "SELECT version FROM sync_queue_schema WHERE id=1").Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version > SchemaVersion {
		return fmt.Errorf("unsupported sync queue schema %d", version)
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS sync_queue_resources (resource_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY) ENGINE=InnoDB`,
		`INSERT INTO sync_queue_resources(resource_key) VALUES('claim_gate') ON DUPLICATE KEY UPDATE resource_key=resource_key`,
		`CREATE TABLE IF NOT EXISTS sync_queue_jobs (
 id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 resource_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 active_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
 account_id VARCHAR(128) NOT NULL, provider_id VARCHAR(128) NOT NULL, region_id VARCHAR(128) NOT NULL,
 scope_type VARCHAR(128) NOT NULL DEFAULT 'region', resource_group VARCHAR(128) NOT NULL DEFAULT 'core',
 range_json MEDIUMTEXT NOT NULL, requested_by VARCHAR(128) NOT NULL, project_id VARCHAR(128) NOT NULL, domain_id VARCHAR(128) NOT NULL,
 state VARCHAR(16) NOT NULL, worker_id VARCHAR(128) NOT NULL DEFAULT '', version BIGINT NOT NULL DEFAULT 0, attempts INT NOT NULL DEFAULT 0,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL, available_at DATETIME(6) NOT NULL, lease_until DATETIME(6) NULL, last_error TEXT NOT NULL, error_class VARCHAR(32) NOT NULL DEFAULT '',
 UNIQUE KEY active_request (active_key), KEY claim_jobs(state,available_at), KEY resource_jobs(resource_key,state), KEY account_jobs(account_id,created_at), KEY account_running(account_id,state,lease_until), KEY provider_running(provider_id,state,lease_until), KEY fair_claim(state,available_at,account_id)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,
		`CREATE TABLE IF NOT EXISTS sync_queue_run_memberships (run_id VARCHAR(128) NOT NULL, job_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, PRIMARY KEY(run_id,job_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,
	} {
		if _, err := q.DB.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := q.ensureQueueIndex(ctx, "sync_queue_run_memberships", "membership_job", "job_id"); err != nil {
		return err
	}
	if err := q.ensureQueueColumn(ctx, "scope_type", "VARCHAR(128) NOT NULL DEFAULT 'region'"); err != nil {
		return err
	}
	if err := q.ensureQueueColumn(ctx, "resource_group", "VARCHAR(128) NOT NULL DEFAULT 'core'"); err != nil {
		return err
	}
	if err := q.ensureQueueColumn(ctx, "error_class", "VARCHAR(32) NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if _, err := q.DB.ExecContext(ctx, "INSERT INTO sync_queue_schema(id,version) VALUES(1,?) ON DUPLICATE KEY UPDATE version=VALUES(version)", SchemaVersion); err != nil {
		return err
	}
	return q.CheckSchema(ctx)
}

func (q *Queue) ensureQueueIndex(ctx context.Context, table, name, column string) error {
	var count int
	if err := q.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND INDEX_NAME=?`, table, name).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	_, err := q.DB.ExecContext(ctx, "ALTER TABLE "+table+" ADD KEY "+name+"("+column+")")
	return err
}

func (q *Queue) ensureQueueColumn(ctx context.Context, name, definition string) error {
	var count int
	if err := q.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='sync_queue_jobs' AND COLUMN_NAME=?`, name).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	_, err := q.DB.ExecContext(ctx, "ALTER TABLE sync_queue_jobs ADD COLUMN "+name+" "+definition)
	return err
}
func (q *Queue) CheckSchema(ctx context.Context) error {
	var version int
	if err := q.DB.QueryRowContext(ctx, "SELECT version FROM sync_queue_schema WHERE id=1").Scan(&version); err != nil {
		return fmt.Errorf("sync queue schema unavailable; run explicit migration: %w", err)
	}
	if version != SchemaVersion {
		return fmt.Errorf("unsupported sync queue schema %d (want %d)", version, SchemaVersion)
	}
	rows, err := q.DB.QueryContext(ctx, "SELECT "+columns+" FROM sync_queue_jobs LIMIT 0")
	if err != nil {
		return err
	}
	rows.Close()
	rows, err = q.DB.QueryContext(ctx, "SELECT resource_key FROM sync_queue_resources LIMIT 0")
	if err != nil {
		return err
	}
	rows.Close()
	rows, err = q.DB.QueryContext(ctx, "SELECT run_id,job_id FROM sync_queue_run_memberships LIMIT 0")
	if err != nil {
		return err
	}
	return rows.Close()
}
func (q *Queue) Enqueue(ctx context.Context, r Request) (string, error) {
	resource, active, err := identity(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	id := fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO sync_queue_resources(resource_key) VALUES(?) ON DUPLICATE KEY UPDATE resource_key=resource_key", resource); err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_queue_jobs(id,resource_key,active_key,account_id,provider_id,region_id,scope_type,resource_group,range_json,requested_by,project_id,domain_id,state,created_at,updated_at,available_at,last_error,error_class) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'waiting',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),'','') ON DUPLICATE KEY UPDATE id=id`, id, resource, active, r.AccountID, r.ProviderID, r.RegionID, r.ScopeType, r.ResourceGroup, r.RangeJSON, r.RequestedBy, r.ProjectID, r.DomainID)
	if err != nil {
		return "", err
	}
	if err = tx.QueryRowContext(ctx, "SELECT id FROM sync_queue_jobs WHERE active_key=?", active).Scan(&id); err != nil {
		return "", err
	}
	if r.RunID != "" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO sync_queue_run_memberships(run_id,job_id) VALUES(?,?) ON DUPLICATE KEY UPDATE job_id=job_id", r.RunID, id); err != nil {
			return "", err
		}
	}
	return id, tx.Commit()
}

const columns = `id,resource_key,account_id,provider_id,region_id,scope_type,resource_group,range_json,requested_by,project_id,domain_id,state,worker_id,version,attempts,created_at,updated_at,available_at,lease_until,last_error,error_class`

type scanner interface{ Scan(...interface{}) error }

func scanJob(row scanner) (*Job, error) {
	j := &Job{}
	err := row.Scan(&j.ID, &j.ResourceKey, &j.AccountID, &j.ProviderID, &j.RegionID, &j.ScopeType, &j.ResourceGroup, &j.RangeJSON, &j.RequestedBy, &j.ProjectID, &j.DomainID, &j.State, &j.WorkerID, &j.Version, &j.Attempts, &j.CreatedAt, &j.UpdatedAt, &j.AvailableAt, &j.LeaseUntil, &j.LastError, &j.ErrorClass)
	return j, err
}

const eligible = `((state IN ('waiting','retry') AND available_at<=UTC_TIMESTAMP(6)) OR (state='running' AND lease_until<=UTC_TIMESTAMP(6)))`

func providerDependencyCondition(candidateAlias string) string {
	return fmt.Sprintf(`NOT EXISTS (
 SELECT 1
 FROM sync_queue_run_memberships candidate_members
 JOIN sync_queue_run_memberships dependency_members ON dependency_members.run_id=candidate_members.run_id
 JOIN sync_queue_jobs dependency ON dependency.id=dependency_members.job_id
 WHERE candidate_members.job_id=%s.id
 AND %s.scope_type IN ('','region')
 AND dependency.scope_type='provider'
 AND dependency.account_id=%s.account_id
 AND dependency.provider_id=%s.provider_id
 AND dependency.state IN ('waiting','running','retry')
)`, candidateAlias, candidateAlias, candidateAlias, candidateAlias)
}

// Claim examines at most 32 resources per call. An empty result can mean temporary
// contention; callers should poll with jitter. Expired leases increment attempts.
func (q *Queue) Claim(ctx context.Context, workerID string, lease time.Duration) (*Job, error) {
	if workerID == "" || len(workerID) > 128 || lease < time.Microsecond {
		return nil, errors.New("invalid worker ID or lease")
	}
	_, accountLimit, providerLimit := q.limits()
	rows, err := q.DB.QueryContext(ctx, `SELECT candidate.resource_key FROM sync_queue_jobs candidate WHERE `+eligible+` AND `+providerDependencyCondition("candidate")+` AND candidate.resource_key NOT IN (SELECT live.resource_key FROM sync_queue_jobs live WHERE live.state='running' AND live.lease_until>UTC_TIMESTAMP(6)) AND (SELECT COUNT(*) FROM sync_queue_jobs a WHERE a.account_id=candidate.account_id AND a.state='running' AND a.lease_until>UTC_TIMESTAMP(6)) < ? AND (SELECT COUNT(*) FROM sync_queue_jobs p WHERE p.provider_id=candidate.provider_id AND p.state='running' AND p.lease_until>UTC_TIMESTAMP(6)) < ? GROUP BY candidate.resource_key,candidate.account_id ORDER BY (SELECT COUNT(*) FROM sync_queue_jobs fair WHERE fair.account_id=candidate.account_id AND fair.state='running' AND fair.lease_until>UTC_TIMESTAMP(6)),MIN(candidate.available_at),candidate.account_id,candidate.resource_key LIMIT 32`, accountLimit, providerLimit)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		job, err := q.claimResource(ctx, key, workerID, lease)
		if err != nil || job != nil {
			return job, err
		}
	}
	return nil, nil
}
func (q *Queue) claimResource(ctx context.Context, key, worker string, lease time.Duration) (*Job, error) {
	tx, err := q.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var locked string
	if err = tx.QueryRowContext(ctx, "SELECT resource_key FROM sync_queue_resources WHERE resource_key='claim_gate' FOR UPDATE").Scan(&locked); err != nil {
		return nil, err
	}
	globalLimit, accountLimit, providerLimit := q.limits()
	var globalRunning int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue_jobs WHERE state='running' AND lease_until>UTC_TIMESTAMP(6)").Scan(&globalRunning); err != nil {
		return nil, err
	}
	if globalRunning >= globalLimit {
		return nil, nil
	}
	if err = tx.QueryRowContext(ctx, "SELECT resource_key FROM sync_queue_resources WHERE resource_key=? FOR UPDATE", key).Scan(&locked); err != nil {
		return nil, err
	}
	var running int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue_jobs WHERE resource_key=? AND state='running' AND lease_until>UTC_TIMESTAMP(6)", key).Scan(&running); err != nil {
		return nil, err
	}
	if running > 0 {
		return nil, nil
	}
	j, err := scanJob(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM sync_queue_jobs candidate_job WHERE candidate_job.resource_key=? AND "+eligible+" AND "+providerDependencyCondition("candidate_job")+" ORDER BY CASE WHEN candidate_job.state='running' THEN 0 ELSE 1 END,candidate_job.available_at,candidate_job.created_at,candidate_job.id LIMIT 1 FOR UPDATE", key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var accountRunning, providerRunning int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue_jobs WHERE account_id=? AND state='running' AND lease_until>UTC_TIMESTAMP(6)", j.AccountID).Scan(&accountRunning); err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue_jobs WHERE provider_id=? AND state='running' AND lease_until>UTC_TIMESTAMP(6)", j.ProviderID).Scan(&providerRunning); err != nil {
		return nil, err
	}
	if accountRunning >= accountLimit || providerRunning >= providerLimit {
		return nil, nil
	}
	result, err := tx.ExecContext(ctx, "UPDATE sync_queue_jobs SET state='running',worker_id=?,version=version+1,attempts=attempts+1,lease_until=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)),updated_at=UTC_TIMESTAMP(6) WHERE id=? AND version=? AND "+eligible, worker, lease.Microseconds(), j.ID, j.Version)
	if err != nil {
		return nil, err
	}
	if err = owned(result); err != nil {
		return nil, err
	}
	j, err = scanJob(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM sync_queue_jobs WHERE id=?", j.ID))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return j, nil
}
func owned(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrLeaseLost
	}
	return nil
}

const owner = `id=? AND worker_id=? AND version=? AND state='running' AND lease_until>UTC_TIMESTAMP(6)`

func (q *Queue) Heartbeat(ctx context.Context, j *Job, lease time.Duration) error {
	if j == nil || lease < time.Microsecond {
		return errors.New("invalid heartbeat")
	}
	result, err := q.DB.ExecContext(ctx, "UPDATE sync_queue_jobs SET lease_until=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)),updated_at=UTC_TIMESTAMP(6) WHERE "+owner, lease.Microseconds(), j.ID, j.WorkerID, j.Version)
	if err != nil {
		return err
	}
	return owned(result)
}
func (q *Queue) Complete(ctx context.Context, j *Job, executionErr error, maxAttempts int, retryDelay time.Duration) error {
	if j == nil || maxAttempts < 1 || retryDelay < 0 {
		return errors.New("invalid completion")
	}
	state := Succeeded
	message := ""
	errorClass := ErrorClass("")
	if executionErr != nil {
		message = executionErr.Error()
		if len(message) > 8192 {
			message = message[:8192]
		}
		errorClass = ClassifyError(executionErr)
		state = Failed
		if j.Attempts < maxAttempts && Retryable(errorClass) {
			state = Retry
		}
	}
	active := "active_key"
	if state != Retry {
		active = "NULL"
	}
	result, err := q.DB.ExecContext(ctx, "UPDATE sync_queue_jobs SET state=?,last_error=?,error_class=?,active_key="+active+",worker_id='',lease_until=NULL,available_at=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)),updated_at=UTC_TIMESTAMP(6) WHERE "+owner, state, message, errorClass, retryDelay.Microseconds(), j.ID, j.WorkerID, j.Version)
	if err != nil {
		return err
	}
	return owned(result)
}
func (q *Queue) Get(ctx context.Context, id string) (*Job, error) {
	return scanJob(q.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM sync_queue_jobs WHERE id=?", id))
}

// List returns newest jobs, bounded to 1000. Empty accountID selects all accounts;
// authorization must be performed by the caller before invoking admin queries.
func (q *Queue) List(ctx context.Context, accountID string, limit int) ([]Job, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	where := ""
	args := []interface{}{}
	if accountID != "" {
		where = " WHERE account_id=?"
		args = append(args, accountID)
	}
	args = append(args, limit)
	rows, err := q.DB.QueryContext(ctx, "SELECT "+columns+" FROM sync_queue_jobs"+where+" ORDER BY created_at DESC,id DESC LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}
func (q *Queue) Stats(ctx context.Context, accountID string) (map[string]int, error) {
	where := ""
	args := []interface{}{}
	if accountID != "" {
		where = " WHERE account_id=?"
		args = append(args, accountID)
	}
	rows, err := q.DB.QueryContext(ctx, "SELECT state,COUNT(*) FROM sync_queue_jobs"+where+" GROUP BY state", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stats := map[string]int{Waiting: 0, Running: 0, Retry: 0, Succeeded: 0, Failed: 0}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		stats[state] = count
	}
	return stats, rows.Err()
}

// ActiveProviderJobs counts non-terminal provider-scoped jobs for one
// provider. Region jobs are reflected by their provider-region status.
func (q *Queue) ActiveProviderJobs(ctx context.Context, providerID string) (int, error) {
	if strings.TrimSpace(providerID) == "" {
		return 0, errors.New("provider ID is required")
	}
	counts, err := q.ActiveProviderJobCounts(ctx, []string{providerID})
	return counts[providerID], err
}

func (q *Queue) ActiveProviderJobCounts(ctx context.Context, providerIDs []string) (map[string]int, error) {
	counts := map[string]int{}
	unique := make([]string, 0, len(providerIDs))
	for _, providerID := range providerIDs {
		providerID = strings.TrimSpace(providerID)
		if providerID == "" || len(providerID) > 128 {
			return nil, errors.New("provider IDs must be 1..128 bytes")
		}
		if _, ok := counts[providerID]; ok {
			continue
		}
		counts[providerID] = 0
		unique = append(unique, providerID)
	}
	if len(unique) == 0 {
		return counts, nil
	}
	args := make([]interface{}, len(unique))
	placeholders := make([]string, len(unique))
	for i, providerID := range unique {
		args[i] = providerID
		placeholders[i] = "?"
	}
	rows, err := q.DB.QueryContext(ctx,
		"SELECT provider_id,COUNT(*) FROM sync_queue_jobs WHERE provider_id IN ("+strings.Join(placeholders, ",")+") AND scope_type='provider' AND state IN ('waiting','running','retry') GROUP BY provider_id",
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var providerID string
		var count int
		if err := rows.Scan(&providerID, &count); err != nil {
			return nil, err
		}
		counts[providerID] = count
	}
	return counts, rows.Err()
}

// StatsRun includes coalesced requests linked to the same durable account run.
func (q *Queue) StatsRun(ctx context.Context, runID string) (map[string]int, error) {
	if runID == "" {
		return nil, errors.New("run ID is required")
	}
	rows, err := q.DB.QueryContext(ctx, "SELECT j.state,COUNT(*) FROM sync_queue_jobs j JOIN sync_queue_run_memberships m ON m.job_id=j.id WHERE m.run_id=? GROUP BY j.state", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stats := map[string]int{Waiting: 0, Running: 0, Retry: 0, Succeeded: 0, Failed: 0}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		stats[state] = count
	}
	return stats, rows.Err()
}
