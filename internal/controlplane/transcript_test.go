package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestMemoryTranscriptStoreIdempotencyAndConcurrentOrder(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-a", UID: "run-uid"}
	input := AppendTranscriptInput{Source: "adapter", IdempotencyKey: "retry", Type: "output", Data: json.RawMessage(`{"text":"same"}`)}

	const retries = 32
	results := make(chan AppendTranscriptResult, retries)
	errorsCh := make(chan error, retries)
	var wait sync.WaitGroup
	for range retries {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := store.Append(context.Background(), run, input)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	created := 0
	var cursor string
	for result := range results {
		if !result.Replayed {
			created++
		}
		if cursor == "" {
			cursor = result.Event.ID
		} else if result.Event.ID != cursor {
			t.Fatalf("idempotent cursor = %q, want %q", result.Event.ID, cursor)
		}
	}
	if created != 1 || len(store.runs[run].events) != 1 {
		t.Fatalf("created=%d retained=%d, want one", created, len(store.runs[run].events))
	}
	conflict := input
	conflict.Data = json.RawMessage(`{"text":"different"}`)
	if _, err := store.Append(context.Background(), run, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting retry error = %v, want %v", err, ErrIdempotencyConflict)
	}

	const appends = 64
	sequences := make(chan uint64, appends)
	for index := range appends {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := store.Append(context.Background(), run, AppendTranscriptInput{
				Source: "adapter", IdempotencyKey: fmt.Sprintf("event-%d", index),
				Type: "output", Data: json.RawMessage(`{}`),
			})
			if err != nil {
				t.Errorf("append: %v", err)
				return
			}
			sequences <- result.Event.Sequence
		}()
	}
	wait.Wait()
	close(sequences)
	ordered := make([]int, 0, appends)
	for sequence := range sequences {
		ordered = append(ordered, int(sequence))
	}
	sort.Ints(ordered)
	for index, sequence := range ordered {
		if want := index + 2; sequence != want {
			t.Fatalf("ordered sequence[%d] = %d, want %d", index, sequence, want)
		}
	}
}

func TestTranscriptStoreInternalErrorsAreGenericAndRetriable(t *testing.T) {
	response := httptest.NewRecorder()
	writeTranscriptStoreError(response, errors.New("connect postgres.internal as private-user: schema detail"))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if body := response.Body.String(); strings.Contains(body, "postgres.internal") || strings.Contains(body, "private-user") || !strings.Contains(body, "transcript store is unavailable") {
		t.Fatalf("unsafe internal store problem response: %s", body)
	}
}

func TestMemoryTranscriptStoreTenantIdentityAndRetentionGap(t *testing.T) {
	options := DefaultMemoryTranscriptStoreOptions()
	options.MaxEventsPerRun = 2
	options.MaxReplayEvents = 2
	store := NewMemoryTranscriptStore(options).(*memoryTranscriptStore)
	runA := RunIdentity{Namespace: "project-a", UID: "shared-uid"}
	runB := RunIdentity{Namespace: "project-b", UID: "shared-uid"}

	first := appendStoreEvent(t, store, runA, "first")
	appendStoreEvent(t, store, runA, "second")
	appendStoreEvent(t, store, runA, "third")
	appendStoreEvent(t, store, runA, "fourth")
	other := appendStoreEvent(t, store, runB, "first")
	if other.Event.Sequence != 1 {
		t.Fatalf("other tenant sequence = %d, want 1", other.Event.Sequence)
	}

	_, err := store.Subscribe(context.Background(), runA, first.Event.ID)
	if !errors.Is(err, ErrExpiredCursor) {
		t.Fatalf("expired cursor error = %v, want %v", err, ErrExpiredCursor)
	}
	var cursorError *TranscriptCursorError
	if !errors.As(err, &cursorError) || cursorError.Gap == nil || cursorError.Gap.EarliestSequence != 3 {
		t.Fatalf("expired cursor recovery = %#v", cursorError)
	}
	recovered, err := store.Subscribe(context.Background(), runA, cursorError.Gap.ResumeAfter)
	if err != nil {
		t.Fatalf("subscribe from recovery boundary: %v", err)
	}
	if len(recovered.History) != 2 || recovered.History[0].Sequence != 3 {
		t.Fatalf("recovery history = %#v", recovered.History)
	}
	recovered.Unsubscribe()
	subscription, err := store.Subscribe(context.Background(), runA, "")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if subscription.Gap == nil || len(subscription.History) != 2 || subscription.History[0].Sequence != 3 {
		t.Fatalf("gap=%#v history=%#v", subscription.Gap, subscription.History)
	}

	reused := appendStoreEvent(t, store, runA, "first")
	if reused.Replayed || reused.Event.Sequence != 5 {
		t.Fatalf("evicted idempotency result = %#v, want new sequence 5", reused)
	}
}

func TestMemoryTranscriptStoreAtomicReplayLiveCut(t *testing.T) {
	for iteration := range 100 {
		store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
		run := RunIdentity{Namespace: "project-a", UID: types.UID(fmt.Sprintf("run-%d", iteration))}
		start := make(chan struct{})
		result := make(chan TranscriptSubscription, 1)
		errorsCh := make(chan error, 2)
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			subscription, err := store.Subscribe(context.Background(), run, "")
			if err != nil {
				errorsCh <- err
				return
			}
			result <- subscription
		}()
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Append(context.Background(), run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "event", Type: "output", Data: json.RawMessage(`{}`)})
			if err != nil {
				errorsCh <- err
			}
		}()
		close(start)
		wait.Wait()
		var subscription TranscriptSubscription
		select {
		case err := <-errorsCh:
			t.Fatal(err)
		case subscription = <-result:
		}
		count := len(subscription.History)
		select {
		case <-subscription.Events:
			count++
		default:
		}
		subscription.Unsubscribe()
		if count != 1 {
			t.Fatalf("iteration %d observed event %d times", iteration, count)
		}
	}
}

func TestMemoryTranscriptStoreConcurrentLiveOrder(t *testing.T) {
	store := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-a", UID: "run-uid"}
	subscription, err := store.Subscribe(context.Background(), run, "")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()

	const appends = 64
	var wait sync.WaitGroup
	for index := range appends {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, appendErr := store.Append(context.Background(), run, AppendTranscriptInput{
				Source: "adapter", IdempotencyKey: fmt.Sprintf("event-%d", index), Type: "output", Data: json.RawMessage(`{}`),
			})
			if appendErr != nil {
				t.Errorf("append: %v", appendErr)
			}
		}()
	}
	wait.Wait()
	for want := uint64(1); want <= appends; want++ {
		event := <-subscription.Events
		if event.Sequence != want {
			t.Fatalf("live sequence = %d, want %d", event.Sequence, want)
		}
	}
}

func TestMemoryTranscriptStoreRejectsInvalidAndRestartedCursors(t *testing.T) {
	run := RunIdentity{Namespace: "project-a", UID: "run-uid"}
	firstStore := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)
	event := appendStoreEvent(t, firstStore, run, "first")
	secondStore := NewMemoryTranscriptStore(MemoryTranscriptStoreOptions{}).(*memoryTranscriptStore)

	if _, err := secondStore.Subscribe(context.Background(), run, event.Event.ID); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("restart cursor error = %v, want %v", err, ErrInvalidCursor)
	}
	wrongRun := RunIdentity{Namespace: run.Namespace, UID: "other-uid"}
	if _, err := firstStore.Subscribe(context.Background(), wrongRun, event.Event.ID); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("wrong-run cursor error = %v, want %v", err, ErrInvalidCursor)
	}
	if _, err := firstStore.Subscribe(context.Background(), run, "not-a-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("malformed cursor error = %v, want %v", err, ErrInvalidCursor)
	}
}

func TestMemoryTranscriptStoreEnforcesReplaySubscriberAndAggregateBounds(t *testing.T) {
	options := DefaultMemoryTranscriptStoreOptions()
	options.MaxRuns = 1
	options.MaxTotalEvents = 3
	options.MaxEventsPerRun = 3
	options.MaxReplayEvents = 1
	options.MaxSubscribers = 1
	options.MaxSubscribersPerRun = 1
	options.SubscriberBuffer = 1
	store := NewMemoryTranscriptStore(options).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-a", UID: "run-uid"}
	appendStoreEvent(t, store, run, "first")
	appendStoreEvent(t, store, run, "second")
	if _, err := store.Subscribe(context.Background(), run, ""); !errors.Is(err, ErrReplayLimit) {
		t.Fatalf("replay error = %v, want %v", err, ErrReplayLimit)
	}

	cursor := store.cursor(run, 1)
	subscription, err := store.Subscribe(context.Background(), run, cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if _, err := store.Subscribe(context.Background(), run, cursor); !errors.Is(err, ErrSubscriberCapacity) {
		t.Fatalf("subscriber error = %v, want %v", err, ErrSubscriberCapacity)
	}
	appendStoreEvent(t, store, run, "third")
	appendStoreEvent(t, store, run, "fourth")
	select {
	case <-subscription.Dropped:
	default:
		t.Fatal("slow subscriber was not dropped")
	}
	if _, err := store.Subscribe(context.Background(), run, store.cursor(run, 3)); !errors.Is(err, ErrSubscriberCapacity) {
		t.Fatalf("dropped subscriber released capacity before unsubscribe: %v", err)
	}
	other := RunIdentity{Namespace: "project-b", UID: "other-uid"}
	if _, err := store.Append(context.Background(), other, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "first", Type: "output", Data: json.RawMessage(`{}`)}); !errors.Is(err, ErrTranscriptCapacity) {
		t.Fatalf("run capacity error = %v, want %v", err, ErrTranscriptCapacity)
	}
}

func TestMemoryTranscriptStoreIdempotencyTupleAndByteBounds(t *testing.T) {
	options := DefaultMemoryTranscriptStoreOptions()
	options.MaxBytesPerRun = 300
	options.MaxTotalBytes = 600
	store := NewMemoryTranscriptStore(options).(*memoryTranscriptStore)
	run := RunIdentity{Namespace: "project-a", UID: "run-uid"}
	first, err := store.Append(context.Background(), run, AppendTranscriptInput{
		Source: "a\x00b", IdempotencyKey: "c", Type: "output", Data: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(context.Background(), run, AppendTranscriptInput{
		Source: "a", IdempotencyKey: "b\x00c", Type: "output", Data: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Event.Sequence == second.Event.Sequence {
		t.Fatal("distinct source/idempotency tuples collided")
	}
	exact, err := store.Append(context.Background(), run, AppendTranscriptInput{
		Source: "a", IdempotencyKey: "b\x00c", Type: "output", Data: json.RawMessage(`{}`),
	})
	if err != nil || !exact.Replayed {
		t.Fatalf("exact retry at capacity = %#v, %v", exact, err)
	}
	if _, err := store.Append(context.Background(), run, AppendTranscriptInput{
		Source: "adapter", IdempotencyKey: "oversized", Type: "output", Data: json.RawMessage(`"` + fmt.Sprintf("%050d", 0) + `"`),
	}); !errors.Is(err, ErrTranscriptCapacity) {
		t.Fatalf("byte capacity error = %v, want %v", err, ErrTranscriptCapacity)
	}
}

func appendStoreEvent(t *testing.T, store TranscriptStore, run RunIdentity, key string) AppendTranscriptResult {
	t.Helper()
	result, err := store.Append(context.Background(), run, AppendTranscriptInput{
		Source: "adapter", IdempotencyKey: key, Type: "output", Data: json.RawMessage(`{"text":"value"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
