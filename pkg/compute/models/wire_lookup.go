package models

import (
	"database/sql"
	"yunion.io/x/onecloud/pkg/httperrors"
	"yunion.io/x/pkg/errors"
)

func selectWireByExternalID(wires []SWire, extID string, vpcIDs []string) (*SWire, error) {
	// nil means legacy unscoped lookup; a non-nil empty list means that the
	// supplied remote VPC has no local match and must never broaden the lookup.
	if vpcIDs != nil {
		matched := make([]SWire, 0, len(wires))
		for _, wire := range wires {
			for _, vpcID := range vpcIDs {
				if wire.VpcId == vpcID {
					matched = append(matched, wire)
					break
				}
			}
		}
		wires = matched
	}
	switch len(wires) {
	case 0:
		return nil, errors.Wrap(sql.ErrNoRows, "not found")
	case 1:
		return &wires[0], nil
	default:
		return nil, errors.Wrapf(httperrors.ErrDuplicateId, "duplicate wires externalId %s", extID)
	}
}
