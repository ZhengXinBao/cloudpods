package models

import "fmt"

// independentRegionCandidates selects the requested scopes before enqueuing.
func independentRegionCandidates(regions []SCloudproviderregion, requested []string) ([]SCloudproviderregion, error) {
	var selected []SCloudproviderregion
	found := make(map[string]bool, len(requested))
	for _, region := range regions {
		if !region.Enabled {
			continue
		}
		if len(requested) == 0 {
			if region.shouldFullSync() || !region.LastSync.IsZero() {
				selected = append(selected, region)
			}
			continue
		}
		for _, id := range requested {
			if region.CloudregionId == id {
				selected = append(selected, region)
				found[id] = true
				break
			}
		}
	}
	for _, id := range requested {
		if !found[id] {
			return nil, fmt.Errorf("requested sync region %s is missing or disabled for provider", id)
		}
	}
	return selected, nil
}
