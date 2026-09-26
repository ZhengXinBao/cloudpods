package syncworker

import "testing"

func TestAccountRouting(t *testing.T) {
	for _, tc := range []struct {
		enabled  bool
		accounts []string
		id       string
		want     bool
	}{
		{false, []string{"a"}, "a", false},
		{true, nil, "a", false},
		{true, []string{"a"}, "a", true},
		{true, []string{"a"}, "b", false},
		{true, []string{"*"}, "a", true},
		{true, []string{"*"}, "", false},
	} {
		if got := RoutesAccount(tc.enabled, tc.accounts, tc.id); got != tc.want {
			t.Fatalf("%+v got %v", tc, got)
		}
	}
}
