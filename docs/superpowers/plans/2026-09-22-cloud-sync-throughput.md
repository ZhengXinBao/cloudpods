# Cloud account sync throughput plan

## Goal

Make the durable cloud-account sync path converge faster and avoid spending the queue budget retrying permanent cloud/API errors. Keep MySQL as the queue and preserve the existing legacy path by default.

## Implementation

1. Add durable queue metadata for `scope_type`, `resource_group`, and `error_class`; keep old rows readable and migrate additively.
2. Split independent region work into a small set of safe resource groups. Keep network/core work serialized per provider-region; run storage/database/edge groups independently only when the requested range permits it.
3. Classify authentication, permission, disabled-region, unsupported, timeout, throttling, and temporary server errors. Permanent/unsupported failures complete immediately; transient failures use bounded jittered retry.
4. Replace the single queue claim gate with a small set of deterministic gates while retaining a short global admission section, and bias claims toward accounts with the least active work.
5. Add tests first for plan expansion, identity isolation, error policy, migration shape, and fair claim ordering. Run package/race/compile checks before reporting completion.

## Deliberate limits

No new broker, no full rewrite of `DoSync`, and no live deployment to the test environment in this change. The existing provider-region mutex remains for the core group because legacy reconcilers are not proven safe for concurrent writes.
