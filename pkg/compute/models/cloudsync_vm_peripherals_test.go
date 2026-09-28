// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

import (
	"testing"
	"time"
)

func TestNeedPeriodicDeepSync(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		last time.Time
		want bool
	}{
		{"never", time.Time{}, true},
		{"recent", now.Add(-time.Hour), false},
		{"stale", now.Add(-25 * time.Hour), true},
	}
	for _, c := range cases {
		if got := needPeriodicDeepSync(c.last, now); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestNeedSyncVMPeripherals(t *testing.T) {
	cases := []struct {
		name     string
		isNew    bool
		deep     bool
		nicCount int
		want     bool
	}{
		{"new vm", true, false, 0, true},
		{"deep sync", false, true, 2, true},
		{"existing vm without nic", false, false, 0, true},
		{"existing vm with nic", false, false, 1, false},
	}
	for _, c := range cases {
		if got := needSyncVMPeripherals(c.isNew, c.deep, c.nicCount); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
