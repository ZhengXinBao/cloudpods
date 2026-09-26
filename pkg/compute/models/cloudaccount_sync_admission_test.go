package models

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAccountManualPreparationWaitsForProbe(t *testing.T) {
	var admission cloudaccountSyncAdmission
	releaseProbe, err := admission.acquire(context.Background(), "account", false)
	if err != nil || releaseProbe == nil {
		t.Fatalf("probe admission: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		releaseManual, err := admission.acquire(ctx, "account", true)
		if releaseManual != nil {
			releaseManual()
		}
		result <- err
	}()
	<-started
	select {
	case err := <-result:
		releaseProbe()
		t.Fatalf("manual preparation returned before probe finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseProbe()
	if err := <-result; err != nil {
		t.Fatalf("manual preparation failed after probe: %v", err)
	}
	releaseNext, err := admission.acquire(ctx, "account", false)
	if err != nil || releaseNext == nil {
		t.Fatalf("admission not released: %v", err)
	}
	releaseNext()
}

func TestAccountWaitingPreparationCancellation(t *testing.T) {
	var admission cloudaccountSyncAdmission
	release, _ := admission.acquire(context.Background(), "account", false)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	acquired, err := admission.acquire(ctx, "account", true)
	if acquired != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait: release=%v error=%v", acquired != nil, err)
	}
	duplicate, err := admission.acquire(context.Background(), "account", false)
	if duplicate != nil || err != nil {
		t.Fatalf("cancellation released active probe: %v", err)
	}
}

func TestAccountBackgroundDuplicateAndOtherAccount(t *testing.T) {
	var admission cloudaccountSyncAdmission
	release, _ := admission.acquire(context.Background(), "account", false)
	defer release()
	duplicate, err := admission.acquire(context.Background(), "account", false)
	if duplicate != nil || err != nil {
		t.Fatalf("duplicate background task admitted: %v", err)
	}
	other, err := admission.acquire(context.Background(), "other", true)
	if other == nil || err != nil {
		t.Fatalf("unrelated account blocked: %v", err)
	}
	other()
}

func TestAccountCanceledPreparationDoesNotAcquireIdleSlot(t *testing.T) {
	var admission cloudaccountSyncAdmission
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release, err := admission.acquire(ctx, "account", true)
	if release != nil {
		release()
	}
	if release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled task admitted: %v", err)
	}
}

func TestAccountWaitingPreparationStopsOnCancellation(t *testing.T) {
	var admission cloudaccountSyncAdmission
	release, _ := admission.acquire(context.Background(), "account", false)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		acquired, err := admission.acquire(ctx, "account", true)
		if acquired != nil {
			acquired()
		}
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected wait result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not stop")
	}
}

func TestAccountResultDeliveryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		deliverCloudaccountSyncResult(ctx, make(chan error), nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("result delivery leaked after receiver canceled")
	}
}

func TestAccountResultDeliveryPreservesPreparationError(t *testing.T) {
	expected := errors.New("region preparation failed")
	result := make(chan error, 1)
	deliverCloudaccountSyncResult(context.Background(), result, expected)
	if got := <-result; got != expected {
		t.Fatalf("preparation error lost: %v", got)
	}
}

func TestCanceledAccountWaiterDoesNotOwnSyncCleanup(t *testing.T) {
	var admission cloudaccountSyncAdmission
	release, _ := admission.acquire(context.Background(), "account", false)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := admission.acquire(ctx, "account", true)
	if !CloudaccountSyncNotAdmitted(err) {
		t.Fatalf("canceled waiter may clean another preparation: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause lost: %v", err)
	}
	if CloudaccountSyncNotAdmitted(context.Canceled) {
		t.Fatal("admitted cancellation must retain cleanup")
	}
}

func TestCanceledAccountResultIsDeliveredToBufferedWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 100; i++ {
		result := make(chan error, 1)
		deliverCloudaccountSyncResult(ctx, result, context.Canceled)
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
		default:
			t.Fatal("cancellation dropped buffered completion; draining caller hangs")
		}
	}
}

func TestCanceledAdmittedAccountSyncDrainsPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	returned := make(chan error, 1)
	go func() { returned <- waitCloudaccountSyncResult(ctx, result) }()
	cancel()
	select {
	case err := <-returned:
		t.Fatalf("cleanup can start before preparation drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	result <- nil
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("completed preparation did not release waiter")
	}
}
