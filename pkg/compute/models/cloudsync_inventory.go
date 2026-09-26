package models

import (
	"fmt"
	"strconv"
	"strings"

	"yunion.io/x/sqlchemy"
)

// verifyCloudInventory checks presence after reconciliation using the same fully
// fetched cloud inventory. IDs are opaque: do not normalize case or whitespace.
// Extra local IDs are not deletions; this audit never mutates resources. Reading
// persisted IDs also includes common records intentionally not updated by Xor.
func verifyCloudInventory(expected []string, persisted func() ([]string, error)) error {
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		if id == "" {
			return fmt.Errorf("cloud inventory contains empty external ID")
		}
		want[id] = struct{}{}
	}
	ids, err := persisted()
	if err != nil {
		return fmt.Errorf("read persisted cloud inventory: %w", err)
	}
	for _, id := range ids {
		delete(want, id)
	}
	if len(want) == 0 {
		return nil
	}
	samples := make([]string, 0, 8)
	// Traverse cloud order for stable, bounded diagnostics, deduplicating samples.
	missing := len(want)
	for _, id := range expected {
		if _, ok := want[id]; !ok {
			continue
		}
		sample := strconv.Quote(id)
		if len(sample) > 96 {
			sample = sample[:96] + "..."
		}
		samples = append(samples, sample)
		delete(want, id)
		if len(samples) == 8 {
			break
		}
	}
	return fmt.Errorf("cloud inventory incomplete: missing=%d sample=[%s]", missing, strings.Join(samples, ", "))
}

// verifyCloudInventoryQuery selects IDs only; callers pass the same provider and
// region scope used for reconciliation. It does not apply a remote fetch limit.
func verifyCloudInventoryQuery(expected []string, q *sqlchemy.SQuery) error {
	return verifyCloudInventory(expected, func() ([]string, error) {
		q.ResetFields().AppendField(q.Field("external_id"))
		rows, err := q.Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		ids := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		// sqlchemy.All omits this check; a truncated database inventory is not
		// evidence of completeness even if all expected IDs appeared first.
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return ids, nil
	})
}
