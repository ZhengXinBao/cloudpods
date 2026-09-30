package models

import (
	"context"
	"errors"
	"fmt"
)

type cloudProviderWork struct {
	providerID string
	run        func(context.Context) error
}

// runCloudProviderWork admits only one step per provider. A blocked SDK call
// therefore occupies one slot, not all the slots of its waiting siblings.
// Cancellation stops admission but must drain calls that cannot be canceled.
func runCloudProviderWork(ctx context.Context, jobs []cloudProviderWork, concurrency int, onDone func(context.Context, string) error) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	type group struct {
		id    string
		ctx   context.Context
		steps []func(context.Context) error
	}
	groups := map[string]*group{}
	ready := []*group{}
	for _, job := range jobs {
		g := groups[job.providerID]
		if g == nil {
			// etcd locks use context identity for reentrancy. Distinct
			// providers must never share an owner, even on the same account.
			providerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			g = &group{id: job.providerID, ctx: providerCtx}
			groups[job.providerID] = g
			ready = append(ready, g)
		}
		if job.run != nil {
			g.steps = append(g.steps, job.run)
		}
	}
	if onDone != nil {
		for _, g := range ready {
			id := g.id
			g.steps = append(g.steps, func(ctx context.Context) error { return onDone(ctx, id) })
		}
	}
	type result struct {
		g   *group
		err error
	}
	done := make(chan result, min(concurrency, len(groups)))
	var errs []error
	active := 0
	for len(ready) > 0 || active > 0 {
		for ctx.Err() == nil && active < concurrency && len(ready) > 0 {
			g := ready[0]
			ready = ready[1:]
			if len(g.steps) == 0 {
				continue
			}
			step := g.steps[0]
			g.steps = g.steps[1:]
			active++
			go func() {
				var err error
				defer func() {
					if p := recover(); p != nil {
						err = fmt.Errorf("provider %s work panicked: %v", g.id, p)
					}
					done <- result{g, err}
				}()
				if err = ctx.Err(); err == nil {
					err = step(g.ctx)
				}
			}()
		}
		if active == 0 {
			break
		}
		r := <-done
		active--
		if r.err != nil {
			errs = append(errs, r.err)
		}
		if len(r.g.steps) > 0 {
			ready = append(ready, r.g)
		}
	}
	return errors.Join(append(errs, ctx.Err())...)
}

// prepareAccountProviderRegions keeps disabled subscriptions out of SDK work.
func prepareAccountProviderRegions(ctx context.Context, providers []SCloudprovider, concurrency int, prepare func(context.Context, *SCloudprovider) error) error {
	jobs := make([]cloudProviderWork, 0, len(providers))
	for i := range providers {
		provider := &providers[i]
		if !provider.GetEnabled() {
			continue
		}
		jobs = append(jobs, cloudProviderWork{providerID: provider.Id, run: func(ctx context.Context) error {
			return prepare(ctx, provider)
		}})
	}
	return runCloudProviderWork(ctx, jobs, concurrency, nil)
}
