package syncqueue

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ActiveAccountJobs lets legacy sync refuse to run beside queued work that was
// routed before the account left the allowlist.
func (q *Queue) ActiveAccountJobs(ctx context.Context, accountID string) (int, error) {
	if strings.TrimSpace(accountID) == "" || len(accountID) > 128 {
		return 0, errors.New("account ID must be 1..128 bytes")
	}
	var count int
	err := q.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue_jobs WHERE account_id=? AND state IN ('waiting','running','retry')", accountID).Scan(&count)
	return count, err
}

// PurgeFinished deletes up to batch terminal jobs whose completion is older than
// retention. Jobs of protected runs are kept: an unfinished run whose jobs vanish
// would read empty progress as success.
func (q *Queue) PurgeFinished(ctx context.Context, retention time.Duration, batch int, protectedRuns []string) (int, error) {
	if retention <= 0 || batch <= 0 || batch > 10000 {
		return 0, errors.New("invalid retention or batch size")
	}
	args := []interface{}{retention.Microseconds()}
	protect := ""
	if len(protectedRuns) > 0 {
		placeholders := make([]string, len(protectedRuns))
		for i, runID := range protectedRuns {
			placeholders[i] = "?"
			args = append(args, runID)
		}
		protect = " AND NOT EXISTS (SELECT 1 FROM sync_queue_run_memberships m WHERE m.job_id=sync_queue_jobs.id AND m.run_id IN (" + strings.Join(placeholders, ",") + "))"
	}
	args = append(args, batch)
	// available_at is stamped at completion, so the claim_jobs index serves this scan.
	rows, err := q.DB.QueryContext(ctx, "SELECT id FROM sync_queue_jobs WHERE state IN ('succeeded','failed') AND available_at<TIMESTAMPADD(MICROSECOND,-?,UTC_TIMESTAMP(6))"+protect+" LIMIT ?", args...)
	if err != nil {
		return 0, err
	}
	var ids []interface{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	in := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Terminal jobs have NULL active_key and never transition again; the state
	// recheck only guards against manual edits between select and delete.
	result, err := tx.ExecContext(ctx, "DELETE FROM sync_queue_jobs WHERE id IN ("+in+") AND state IN ('succeeded','failed')", ids...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sync_queue_run_memberships WHERE job_id IN ("+in+") AND NOT EXISTS (SELECT 1 FROM sync_queue_jobs j WHERE j.id=sync_queue_run_memberships.job_id)", ids...); err != nil {
		return 0, err
	}
	return int(n), tx.Commit()
}
