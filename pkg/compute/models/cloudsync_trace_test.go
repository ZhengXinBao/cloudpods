package models

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"yunion.io/x/onecloud/pkg/compute/synctrace"
	"yunion.io/x/pkg/tristate"
)

type syncTraceCapture struct {
	mu     sync.Mutex
	events []synctrace.Event
}

func (c *syncTraceCapture) context() context.Context {
	ctx := synctrace.WithIdentity(context.Background(), synctrace.Identity{AccountID: "account", RunID: "run"})
	return synctrace.WithSink(ctx, func(e synctrace.Event) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.events = append(c.events, e)
	})
}

func (c *syncTraceCapture) ends(phase string) []synctrace.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var events []synctrace.Event
	for _, e := range c.events {
		if e.Phase == phase && e.Kind != "start" {
			events = append(events, e)
		}
	}
	return events
}

func TestCoreStagePreservesError(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := capture.context()
	want := errors.New("private payload must not appear in trace")
	calls := 0
	got := traceCloudSyncStage(ctx, "core_driver", func() error {
		calls++
		return want
	})
	if got != want || calls != 1 {
		t.Fatalf("got=%v calls=%d", got, calls)
	}
	events := capture.ends("core_driver")
	if len(events) != 1 || events[0].Outcome != "error" {
		t.Fatalf("%+v", events)
	}
	data, err := json.Marshal(capture.events)
	if err != nil || strings.Contains(string(data), want.Error()) {
		t.Fatalf("trace leaked error payload: %s (marshal error: %v)", data, err)
	}
}

func TestCoreStagePreservesCapturedContext(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx, cancel := context.WithCancel(capture.context())
	defer cancel()
	cancel()
	// Diagnostic identity may differ, but SDK/lock work must keep the exact ctx.
	traceCtx := synctrace.WithIdentity(ctx, synctrace.Identity{ProviderID: "provider", RegionID: "region"})
	var gotCtx context.Context
	calls := 0
	err := traceCloudSyncStage(traceCtx, "core_driver", func() error {
		calls++
		gotCtx = ctx
		return nil
	})
	if err != nil || calls != 1 || gotCtx != ctx || gotCtx.Err() != context.Canceled {
		t.Fatalf("err=%v calls=%d context changed=%v", err, calls, gotCtx != ctx)
	}
	if len(capture.events) != 2 || capture.events[0].Kind != "start" {
		t.Fatalf("missing stage start/end: %+v", capture.events)
	}
	end := capture.events[1]
	if end.Kind != "end" || end.Outcome != "ok" || end.AccountID != "account" || end.RunID != "run" || end.ProviderID != "provider" || end.RegionID != "region" {
		t.Fatalf("incorrect success or identity: %+v", end)
	}
}

func TestCoreStageDisabled(t *testing.T) {
	for _, want := range []error{nil, errors.New("private failure"), context.Canceled} {
		var events []synctrace.Event
		ctx := synctrace.WithSink(context.Background(), func(e synctrace.Event) { events = append(events, e) })
		calls := 0
		got := traceCloudSyncStage(ctx, "core_driver", func() error {
			calls++
			return want
		})
		if got != want || calls != 1 || len(events) != 0 {
			t.Fatalf("got=%v want=%v calls=%d events=%+v", got, want, calls, events)
		}
	}
}

func TestCoreStagePreservesPanic(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var events []synctrace.Event
		ctx := synctrace.WithSink(context.Background(), func(e synctrace.Event) { events = append(events, e) })
		if enabled {
			ctx = synctrace.WithIdentity(ctx, synctrace.Identity{JobID: "job"})
		}
		want := errors.New("private panic payload")
		calls := 0
		func() {
			defer func() {
				if got := recover(); got != want {
					t.Errorf("panic changed: got=%v want=%v", got, want)
				}
			}()
			_ = traceCloudSyncStage(ctx, "core_driver", func() error {
				calls++
				panic(want)
			})
		}()
		if calls != 1 {
			t.Fatalf("work ran %d times", calls)
		}
		if enabled {
			if len(events) != 2 || events[1].Kind != "end" || events[1].Outcome != "error" {
				t.Fatalf("missing panic end: %+v", events)
			}
		} else if len(events) != 0 {
			t.Fatalf("disabled trace emitted events: %+v", events)
		}
		data, err := json.Marshal(events)
		if err != nil || strings.Contains(string(data), want.Error()) {
			t.Fatalf("trace leaked panic payload: %s (marshal error: %v)", data, err)
		}
	}
}

func TestCoreObservationPreservesContextAndResults(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var events []synctrace.Event
		ctx := synctrace.WithSink(context.Background(), func(e synctrace.Event) { events = append(events, e) })
		traceCtx := ctx
		if enabled {
			traceCtx = synctrace.WithIdentity(ctx, synctrace.Identity{JobID: "job"})
		}
		want := &SyncResult{}
		want.Error(errors.New("private result error"))
		results := SSyncResultSet{}
		var gotCtx context.Context
		calls := 0
		observeCloudSyncStage(traceCtx, "core_zones", func() {
			calls++
			gotCtx = ctx
			results["zones"] = want
		})
		if calls != 1 || gotCtx != ctx || results["zones"] != want || !results["zones"].IsError() {
			t.Fatalf("work changed: calls=%d context changed=%v results=%+v", calls, gotCtx != ctx, results)
		}
		if enabled {
			if len(events) != 2 || events[0].Kind != "start" || events[1].Kind != "cost" || events[1].Outcome != "" || events[1].ErrorClass != "" {
				t.Fatalf("result-set work must not assert success: %+v", events)
			}
		} else if len(events) != 0 {
			t.Fatalf("disabled trace emitted events: %+v", events)
		}
		data, err := json.Marshal(events)
		if err != nil || strings.Contains(string(data), "private result error") {
			t.Fatalf("trace leaked result payload: %s (marshal error: %v)", data, err)
		}
	}
}

func TestCoreObservationPreservesPanic(t *testing.T) {
	capture := &syncTraceCapture{}
	want := errors.New("private observation panic")
	calls := 0
	func() {
		defer func() {
			if got := recover(); got != want {
				t.Errorf("panic changed: got=%v want=%v", got, want)
			}
		}()
		observeCloudSyncStage(capture.context(), "core_zones", func() {
			calls++
			panic(want)
		})
	}()
	events := capture.ends("core_zones")
	if calls != 1 || len(events) != 1 || events[0].Kind != "cost" || events[0].Outcome != "error" {
		t.Fatalf("calls=%d events=%+v", calls, events)
	}
}

// Helper tests alone cannot catch missing instrumentation in the executor.
// Check real calls in DoSync and its inventory callees, including captured ctx.
func TestCoreStagesWiredIntoDoSync(t *testing.T) {
	functions := map[string]*ast.FuncDecl{}
	for _, path := range []string{"cloudproviderregions.go", "cloudsync.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				functions[fn.Name.Name] = fn
			}
		}
	}
	isIdent := func(expr ast.Expr, name string) bool {
		id, ok := expr.(*ast.Ident)
		return ok && id.Name == name
	}
	callName := func(call *ast.CallExpr) string {
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			return fun.Name
		case *ast.SelectorExpr:
			if receiver, ok := fun.X.(*ast.Ident); ok {
				return receiver.Name + "." + fun.Sel.Name
			}
			return fun.Sel.Name
		}
		return ""
	}
	for _, callee := range []string{"syncPublicCloudProviderInfo", "syncOnPremiseCloudProviderInfo"} {
		calls := 0
		ast.Inspect(functions["DoSync"], func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && callName(call) == callee {
				calls++
				if len(call.Args) == 0 || !isIdent(call.Args[0], "ctx") {
					t.Errorf("DoSync must pass original ctx to %s", callee)
				}
			}
			return true
		})
		if calls != 1 {
			t.Errorf("DoSync must invoke %s once, found %d calls", callee, calls)
		}
	}
	for _, boundary := range []struct {
		function, call, wrapper, phase string
		noContext                      bool
	}{
		{"DoSync", "provider.GetProvider", "traceCloudSyncStage", "core_driver", false},
		{"DoSync", "driver.GetIRegionById", "traceCloudSyncStage", "core_region", true},
		{"syncPublicCloudProviderInfo", "syncRegionZones", "observeCloudSyncStage", "core_zones", false},
		{"syncPublicCloudProviderInfo", "syncRegionSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "SyncRegionNatSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "SyncRegionNasSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "syncSkusFromPrivateCloud", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "syncDBInstanceSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "syncNATSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "syncCacheSkus", "observeCloudSyncStage", "core_skus", false},
		{"syncPublicCloudProviderInfo", "syncZoneStorages", "observeCloudSyncStage", "core_zone_storages", false},
		{"syncPublicCloudProviderInfo", "syncZoneHosts", "observeCloudSyncStage", "core_zone_hosts", false},
		{"syncPublicCloudProviderInfo", "syncRegionSnapshots", "observeCloudSyncStage", "core_snapshots", false},
		{"syncPublicCloudProviderInfo", "syncCloudImages", "observeCloudSyncStage", "core_cached_images", false},
		{"syncOnPremiseCloudProviderInfo", "syncCloudImages", "observeCloudSyncStage", "core_cached_images", false},
	} {
		t.Run(boundary.function+"/"+boundary.call, func(t *testing.T) {
			fn := functions[boundary.function]
			if fn == nil {
				t.Fatalf("missing executor %s", boundary.function)
			}
			var calls []*ast.CallExpr
			var spans []*ast.FuncLit
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if callName(call) == boundary.call {
					calls = append(calls, call)
					if !boundary.noContext && (len(call.Args) == 0 || !isIdent(call.Args[0], "ctx")) {
						t.Error("SDK/lock work must retain original ctx")
					}
				}
				if callName(call) == boundary.wrapper && len(call.Args) == 3 {
					phase, ok := call.Args[1].(*ast.BasicLit)
					if !ok || phase.Kind != token.STRING {
						t.Error("phase must be a bounded string literal")
						return true
					}
					name, err := strconv.Unquote(phase.Value)
					if err != nil || name != boundary.phase {
						return true
					}
					work, ok := call.Args[2].(*ast.FuncLit)
					if !ok || work.Type.Params.NumFields() != 0 || !isIdent(call.Args[0], "ctx") {
						t.Error("span must use a closure capturing original ctx")
						return true
					}
					spans = append(spans, work)
				}
				return true
			})
			if len(calls) != 1 || len(spans) != 1 {
				t.Fatalf("need one actual call inside one %s span: calls=%d spans=%d", boundary.phase, len(calls), len(spans))
			}
			inside := false
			ast.Inspect(spans[0].Body, func(node ast.Node) bool {
				inside = inside || node == calls[0]
				return true
			})
			if !inside {
				t.Fatalf("actual %s call is outside %s span", boundary.call, boundary.phase)
			}
		})
	}
}

func TestTraceProviderPreparation(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := capture.context()
	providers := make([]SCloudprovider, 3)
	for i, id := range []string{"one", "two", "disabled"} {
		providers[i].Id = id
		providers[i].CloudaccountId = "account"
		providers[i].Enabled = tristate.False
		if i != 2 {
			providers[i].Enabled = tristate.True
		}
	}
	want := errors.New("timeout")
	err := prepareAccountProviderRegions(ctx, providers, 2, func(ctx context.Context, p *SCloudprovider) error {
		if p.Id == "two" {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("work result changed: %v", err)
	}
	events := capture.ends("provider_prepare")
	if len(events) != 2 {
		t.Fatalf("missing provider events: %+v", capture.events)
	}
	for _, e := range events {
		if e.AccountID != "account" || e.RunID != "run" || e.ProviderID == "disabled" || (e.Outcome == "error") != (e.ProviderID == "two") {
			t.Fatalf("invalid provider event: %+v", e)
		}
	}
}

func TestTracePlanningPreservesCallbackContextAndFailure(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := capture.context()
	var discovered context.Context
	want := errors.New("access denied")
	err := planIndependentSyncProviders(ctx, SSyncRange{}, "run",
		func(ctx context.Context, ready func(context.Context, *SCloudprovider) error) error {
			discovered = ctx
			p := &SCloudprovider{}
			p.Id = "provider"
			p.CloudaccountId = "account"
			return ready(ctx, p)
		},
		func(ctx context.Context, p *SCloudprovider, sr SSyncRange, run string) error {
			if ctx != discovered {
				t.Error("tracing changed provider lock owner")
			}
			if run != "run" {
				t.Errorf("run changed: %s", run)
			}
			return want
		})
	if !errors.Is(err, want) {
		t.Fatalf("failure lost: %v", err)
	}
	for _, phase := range []string{"discover_plan", "provider_plan"} {
		events := capture.ends(phase)
		if len(events) != 1 || events[0].Outcome != "error" || events[0].RunID != "run" {
			t.Fatalf("missing %s failure: %+v", phase, capture.events)
		}
	}
}

func TestTraceResourceCostsDoNotClaimSuccessfulInventory(t *testing.T) {
	capture := &syncTraceCapture{}
	ctx := synctrace.WithIdentity(capture.context(), synctrace.Identity{JobID: "job", ResourceGroup: "core", Attempt: 2})
	traceSyncResultCosts(ctx, SSyncResultSet{
		"servers": {rc: 2 * time.Second, sc: 3 * time.Second},
		"empty":   {},
		"nil":     nil,
	})
	events := capture.ends("resource_request")
	if len(events) != 1 || events[0].DurationMS != 2000 || events[0].Resource != "servers" || events[0].JobID != "job" || events[0].Outcome != "" {
		t.Fatalf("incorrect request costs: %+v", capture.events)
	}
	events = capture.ends("resource_reconcile")
	if len(events) != 1 || events[0].DurationMS != 3000 || events[0].Outcome != "" {
		t.Fatalf("incorrect reconciliation costs: %+v", capture.events)
	}
}

func TestTraceProbeDrainDoesNotAbandonCanceledSDK(t *testing.T) {
	events := make(chan synctrace.Event, 4)
	ctx, cancel := context.WithCancel(synctrace.WithSink(context.Background(), func(e synctrace.Event) { events <- e }))
	defer cancel()
	drained := make(chan struct{})
	done := make(chan struct{})
	go func() {
		traceCloudProbeDrain(ctx, drained)
		close(done)
	}()
	select {
	case e := <-events:
		if e.Phase != "probe_drain" || e.Kind != "start" {
			t.Errorf("missing drain start: %+v", e)
		}
	case <-time.After(time.Second):
		close(drained)
		t.Fatal("missing drain start")
	}
	cancel()
	select {
	case <-done:
		t.Error("canceled diagnostic scope abandoned SDK")
	case <-time.After(10 * time.Millisecond):
	}
	close(drained)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain did not return")
	}
	e := <-events
	if e.Phase != "probe_drain" || e.Kind != "end" {
		t.Fatalf("missing drain completion: %+v", e)
	}
}

func TestTraceCompletedProbeDoesNotAddDrainEvents(t *testing.T) {
	capture := &syncTraceCapture{}
	done := make(chan struct{})
	close(done)
	traceCloudProbeDrain(capture.context(), done)
	if len(capture.events) != 0 {
		t.Fatalf("already drained probe generated events: %+v", capture.events)
	}
}
