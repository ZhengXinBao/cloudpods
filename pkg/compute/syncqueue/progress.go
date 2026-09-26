package syncqueue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ProgressPagination(offset, limit int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	return offset, limit
}

// PublicError deliberately projects untrusted SDK errors onto a fixed vocabulary.
// SDK messages can embed request bodies, URLs, headers and account credentials.
func PublicError(raw string) string {
	if raw == "" {
		return ""
	}
	if summary := publicInventoryError(raw); summary != "" {
		return summary
	}
	switch ClassifyError(errors.New(raw)) {
	case ErrorClassAuth:
		return "Cloud authentication failed"
	case ErrorClassPermission:
		return "Cloud permission denied"
	case ErrorClassRegionDisabled:
		return "Cloud region is disabled"
	case ErrorClassUnsupported:
		return "Cloud operation is unsupported"
	case ErrorClassThrottle:
		return "Cloud request was rate limited"
	case ErrorClassTimeout:
		return "Cloud request timed out or connection failed"
	case ErrorClassTemporary:
		return "Cloud service is temporarily unavailable"
	default:
		return "Resource synchronization failed; inspect restricted service logs for details"
	}
}

func RetryRequest(job Job, accountID, runID string) (Request, error) {
	if job.AccountID != accountID || accountID == "" {
		return Request{}, errors.New("sync job account binding changed")
	}
	if job.State != Failed {
		return Request{}, errors.New("only failed jobs can be retried")
	}
	r := job.Request
	r.RunID = runID
	if _, _, err := identity(r); err != nil {
		return Request{}, err
	}
	return r, nil
}

// RunJobs is account scoped even after the caller has checked task ownership.
// limit=0 is reserved for the internal retry planner; API callers always paginate.
func (q *Queue) RunJobs(ctx context.Context, accountID, runID string, offset, limit int, failedOnly bool) ([]Job, error) {
	if accountID == "" || runID == "" {
		return nil, errors.New("account and run are required")
	}
	cols := "j." + strings.ReplaceAll(columns, ",", ",j.")
	query := "SELECT " + cols + " FROM sync_queue_jobs j JOIN sync_queue_run_memberships m ON m.job_id=j.id WHERE j.account_id=? AND m.run_id=?"
	args := []interface{}{accountID, runID}
	if failedOnly {
		query += " AND j.state='failed'"
	}
	query += " ORDER BY j.created_at,j.id"
	if limit > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, limit, offset)
	}
	rows, err := q.DB.QueryContext(ctx, query, args...)
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

func EmptyProgressCounts() map[string]int {
	return map[string]int{Waiting: 0, Running: 0, Retry: 0, Succeeded: 0, Failed: 0, "total": 0}
}

func (q *Queue) RunProgressCounts(ctx context.Context, accountID, runID string) (map[string]int, map[string]int, error) {
	counts, regions := EmptyProgressCounts(), EmptyProgressCounts()
	rows, err := q.DB.QueryContext(ctx, `SELECT j.provider_id,j.region_id,j.scope_type,j.state,COUNT(*) FROM sync_queue_jobs j JOIN sync_queue_run_memberships m ON m.job_id=j.id WHERE j.account_id=? AND m.run_id=? GROUP BY j.provider_id,j.region_id,j.scope_type,j.state`, accountID, runID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	states := map[[2]string]string{}
	for rows.Next() {
		var p, r, scope, state string
		var n int
		if err := rows.Scan(&p, &r, &scope, &state, &n); err != nil {
			return nil, nil, err
		}
		counts[state] += n
		counts["total"] += n
		if scope == "provider" {
			continue
		}
		key := [2]string{p, r}
		if progressStateRank(state) > progressStateRank(states[key]) {
			states[key] = state
		}
	}
	for _, state := range states {
		regions[state]++
		regions["total"]++
	}
	return counts, regions, rows.Err()
}
func progressStateRank(state string) int {
	switch state {
	case Failed:
		return 5
	case Running:
		return 4
	case Retry:
		return 3
	case Waiting:
		return 2
	case Succeeded:
		return 1
	}
	return 0
}

// Only recognize the local inventory auditor's bounded count and cloud-generated
// identifiers with known formats. Arbitrary SDK text, URLs, headers and other
// opaque strings are never echoed, even when embedded beside an audit message.
var inventorySummaryPattern = regexp.MustCompile(`cloud inventory incomplete: missing=([0-9]{1,10}) sample=\[([^\]\r\n]{0,1000})\]`)
var inventorySamplePattern = regexp.MustCompile(`"([^"]{1,96})"`)
var publicCloudIDPattern = regexp.MustCompile(`^(?:(?:clb|lb|eip|redis|cdb)-[a-z0-9]{8}|(?:i|vol|vpc|subnet|sg|eni|eipalloc|nat|igw|rtb|snap|ami|vpce|pcx)-[a-f0-9]{3,32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$`)

func publicInventoryError(raw string) string {
	if len(raw) > 8192 {
		raw = raw[:8192]
	}
	match := inventorySummaryPattern.FindStringSubmatch(raw)
	if len(match) != 3 {
		return ""
	}
	count, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil || count == 0 {
		return ""
	}
	samples := []string{}
	for _, sample := range inventorySamplePattern.FindAllStringSubmatch(match[2], 8) {
		if publicCloudIDPattern.MatchString(sample[1]) {
			samples = append(samples, sample[1])
		}
	}
	summary := "Cloud inventory incomplete: missing=" + strconv.FormatUint(count, 10)
	if len(samples) > 0 {
		summary += "; sample=[" + strings.Join(samples, ", ") + "]"
	}
	return summary + " (bounded audit summary)"
}

// AcquireRetryLock serializes retries across API replicas even without etcd.
// The lock belongs to a pinned database connection and is released on exit.
func (q *Queue) AcquireRetryLock(ctx context.Context, key string) (func(), error) {
	sum := sha256.Sum256([]byte(key))
	name := "sync-retry:" + hex.EncodeToString(sum[:24])
	conn, err := q.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var acquired sql.NullInt64
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?,10)", name).Scan(&acquired); err != nil || !acquired.Valid || acquired.Int64 != 1 {
		conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("sync retry is busy")
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(releaseCtx, "DO RELEASE_LOCK(?)", name); err != nil {
			conn.Raw(func(interface{}) error { return driver.ErrBadConn })
		}
		conn.Close()
	}, nil
}

// RetryRequestIdentity is stable across run/attempt/terminal-state changes.
func RetryRequestIdentity(r Request) (string, error) { _, key, err := identity(r); return key, err }

// EnqueueRetryRequest is replay safe even after a previous copy has completed.
// Membership insertion and enqueue are atomic; a crash cannot hide a copied job.
func (q *Queue) EnqueueRetryRequest(ctx context.Context, request Request) (string, error) {
	unlock, err := q.AcquireRetryLock(ctx, "copy:"+request.RunID)
	if err != nil {
		return "", err
	}
	defer unlock()
	resource, key, err := identity(request)
	if err != nil {
		return "", err
	}
	cols := "j." + strings.ReplaceAll(columns, ",", ",j.")
	rows, err := q.DB.QueryContext(ctx, "SELECT "+cols+" FROM sync_queue_jobs j JOIN sync_queue_run_memberships m ON m.job_id=j.id WHERE j.account_id=? AND m.run_id=? AND j.resource_key=?", request.AccountID, request.RunID, resource)
	if err != nil {
		return "", err
	}
	var existingID string
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return "", err
		}
		prior, err := RetryRequestIdentity(job.Request)
		if err != nil {
			rows.Close()
			return "", err
		}
		if prior == key {
			existingID = job.ID
			break
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	if existingID != "" {
		return existingID, nil
	}
	return q.Enqueue(ctx, request)
}
