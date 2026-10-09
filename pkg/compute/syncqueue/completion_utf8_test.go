package syncqueue

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// This opt-in DSN must identify a disposable database: queue tables are dropped.
func TestMySQLCompleteUTF8(t *testing.T) {
	dsn := os.Getenv("SYNCQUEUE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set SYNCQUEUE_TEST_MYSQL_DSN to a disposable MySQL database (parseTime=true)")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	q := New(db)
	for _, table := range []string{"sync_queue_run_memberships", "sync_queue_jobs", "sync_queue_resources", "sync_queue_schema"} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func(t *testing.T) {
		t.Helper()
		for _, table := range []string{"sync_queue_run_memberships", "sync_queue_jobs"} {
			if _, err := db.Exec("DELETE FROM " + table); err != nil {
				t.Fatal(err)
			}
		}
	}
	claim := func(t *testing.T) *Job {
		t.Helper()
		id, err := q.Enqueue(ctx, Request{RunID: "utf8-run", AccountID: "utf8-account", ProviderID: "utf8-provider", RegionID: "utf8-region", RangeJSON: `{}`})
		if err != nil {
			t.Fatal(err)
		}
		j, err := q.Claim(ctx, "utf8-worker", time.Minute)
		if err != nil || j == nil || j.ID != id {
			t.Fatalf("claim=%v err=%v", j, err)
		}
		return j
	}
	for _, tc := range []struct {
		name      string
		err       error
		wantText  string
		wantClass ErrorClass
		wantState string
	}{
		{"success", nil, "", "", Succeeded},
		{"short valid", sdkCodeError{"AccessDenied", "权限错误🙂"}, "权限错误🙂", ErrorClassPermission, Failed},
		{"long Chinese", sdkCodeError{"AccessDenied", strings.Repeat("错", 3000)}, strings.Repeat("错", 2730), ErrorClassPermission, Failed},
		{"emoji boundary", sdkCodeError{"AccessDenied", "x" + strings.Repeat("🙂", 3000)}, "x" + strings.Repeat("🙂", 2047), ErrorClassPermission, Failed},
		{"short invalid", sdkCodeError{"AccessDenied", "\xff开始\xfe\xff结束\xe4\xb8"}, "\uFFFD开始\uFFFD结束\uFFFD", ErrorClassPermission, Failed},
		{"exact invalid", sdkCodeError{"AccessDenied", strings.Repeat("a", 8191) + "\xff"}, strings.Repeat("a", 8191), ErrorClassPermission, Failed},
		{"expanding invalid", sdkCodeError{"AccessDenied", strings.Repeat("a\xff", 3000)}, strings.Repeat("a\uFFFD", 2048), ErrorClassPermission, Failed},
		{"raw invalid", errors.New("AccessDenied: \xff"), "AccessDenied: \uFFFD", ErrorClassPermission, Failed},
		{"typed unknown", sdkCodeError{"DecodeFailure", "AuthFailure \xff"}, "AuthFailure \uFFFD", ErrorClassUnknown, Retry},
		{"typed retry", sdkCodeError{"InternalError", "AuthFailure \xff"}, "AuthFailure \uFFFD", ErrorClassTemporary, Retry},
		{"empty failure", errors.New(""), "", ErrorClassUnknown, Retry},
		{"summary", NewErrorSummary(strings.Repeat("错误", 1500), map[ErrorClass]int{ErrorClassPermission: 2}), strings.Repeat("错误", 682) + "..." + "误" + strings.Repeat("错误", 682), ErrorClassPermission, Failed},
		{"merged bypass", errors.Join(sdkCodeError{"InternalError", "\xff原始"}, NewErrorSummary("采集", map[ErrorClass]int{ErrorClassUnknown: 1})), "\uFFFD原始\n采集", ErrorClassTemporary, Retry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			j := claim(t)
			before := time.Now().UTC()
			if err := q.Complete(ctx, j, tc.err, 2, time.Hour); err != nil {
				t.Fatalf("Complete rejected diagnostic: %v", err)
			}
			got, err := q.Get(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !utf8.ValidString(got.LastError) || len(got.LastError) > 8192 || got.LastError != tc.wantText {
				t.Fatalf("stored text: bytes=%d valid=%v wantBytes=%d", len(got.LastError), utf8.ValidString(got.LastError), len(tc.wantText))
			}
			if got.State != tc.wantState || got.ErrorClass != tc.wantClass || got.Attempts != 1 || got.Version != j.Version || got.WorkerID != "" || got.LeaseUntil.Valid {
				t.Fatalf("completion semantics changed: state=%s class=%s attempts=%d version=%d worker=%q lease=%v", got.State, got.ErrorClass, got.Attempts, got.Version, got.WorkerID, got.LeaseUntil.Valid)
			}
			var active sql.NullString
			if err := db.QueryRow("SELECT active_key FROM sync_queue_jobs WHERE id=?", j.ID).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if active.Valid != (tc.wantState == Retry) || active.Valid && active.String == "" {
				t.Fatal("active key retention changed")
			}
			if tc.wantState == Retry {
				if got.AvailableAt.Before(before.Add(time.Hour)) {
					t.Fatal("retry delay lost")
				}
				if early, err := q.Claim(ctx, "too-early", time.Minute); err != nil || early != nil {
					t.Fatalf("retry eligible too soon: %v %v", early, err)
				}
				if _, err := db.Exec("UPDATE sync_queue_jobs SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?", j.ID); err != nil {
					t.Fatal(err)
				}
				retried, err := q.Claim(ctx, "retry-worker", time.Minute)
				if err != nil || retried == nil || retried.ID != j.ID || retried.Attempts != 2 {
					t.Fatalf("retry claim: %v %v", retried, err)
				}
				if err := q.Complete(ctx, retried, tc.err, 2, 0); err != nil {
					t.Fatal(err)
				}
				final, err := q.Get(ctx, j.ID)
				if err != nil || final.State != Failed || final.Attempts != 2 || final.ErrorClass != tc.wantClass || final.LastError != tc.wantText {
					t.Fatalf("retry exhaustion changed: %v", err)
				}
			}
		})
	}
	t.Run("ownership and SQL errors remain visible", func(t *testing.T) {
		reset(t)
		j := claim(t)
		original := *j
		badText := sdkCodeError{"AccessDenied", "\xff"}
		for _, mutate := range []func(*Job){
			func(j *Job) { j.Version++ },
			func(j *Job) { j.WorkerID = "wrong-worker" },
		} {
			stale := *j
			mutate(&stale)
			if err := q.Complete(ctx, &stale, badText, 2, 0); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale owner not rejected: %v", err)
			}
		}
		if _, err := db.Exec("UPDATE sync_queue_jobs SET lease_until=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?", j.ID); err != nil {
			t.Fatal(err)
		}
		if err := q.Complete(ctx, j, badText, 2, 0); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expired lease not rejected: %v", err)
		}
		got, err := q.Get(ctx, j.ID)
		if err != nil || got.State != Running || got.LastError != "" || got.WorkerID != original.WorkerID || got.Version != original.Version || got.Attempts != original.Attempts {
			t.Fatal("rejected completion changed job")
		}
		replacement, err := q.Claim(ctx, "replacement", time.Minute)
		if err != nil || replacement == nil {
			t.Fatalf("reclaim failed: %v", err)
		}
		if err := q.Complete(ctx, replacement, nil, 2, 0); err != nil {
			t.Fatal(err)
		}
		if err := q.Complete(ctx, replacement, badText, 2, 0); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("terminal job overwritten: %v", err)
		}
		closed, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		closed.Close()
		if err := New(closed).Complete(ctx, replacement, badText, 2, 0); err == nil || errors.Is(err, ErrLeaseLost) {
			t.Fatalf("real SQL error was suppressed: %v", err)
		}
	})
}
