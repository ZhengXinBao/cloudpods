package cloudaccount

import (
	"testing"
	"yunion.io/x/jsonutils"
)

func TestIndependentRunCompletion(t *testing.T) {
	for _, tc := range []struct {
		name         string
		counts       map[string]int
		planError    bool
		done, failed bool
	}{
		{"empty sealed run", map[string]int{}, false, true, false},
		{"empty failed planning", map[string]int{}, true, true, true},
		{"queued", map[string]int{"waiting": 1}, false, false, false},
		{"running", map[string]int{"running": 1, "succeeded": 3}, false, false, false},
		{"retry", map[string]int{"retry": 1}, false, false, false},
		{"partial failure waits", map[string]int{"failed": 1, "running": 1}, false, false, false},
		{"failure", map[string]int{"failed": 1, "succeeded": 2}, false, true, true},
		{"planning failed with jobs", map[string]int{"waiting": 1}, true, false, false},
		{"planning failed after jobs", map[string]int{"succeeded": 1}, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := jsonutils.NewDict()
			if tc.planError {
				params.Set("independent_sync_plan_error", jsonutils.NewString("database read failed"))
			}
			done, data := independentRunResult(tc.counts, params)
			if done != tc.done {
				t.Fatalf("done=%v want %v", done, tc.done)
			}
			failed := false
			if data != nil {
				status, _ := data.GetString("__status__")
				failed = status == "ERROR"
			}
			if failed != tc.failed {
				t.Fatalf("failed=%v want %v", failed, tc.failed)
			}
		})
	}
}
