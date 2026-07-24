package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/types"
)

func TestPostgresTranscriptStoreContract(t *testing.T) {
	baseDatabaseURL := os.Getenv("SWE_TEST_POSTGRES_URL")
	if baseDatabaseURL == "" {
		t.Skip("SWE_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, baseDatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("transcript_test_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		t.Fatalf("create isolated transcript test schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		pool.Close()
	})
	parsedDatabaseURL, err := url.Parse(baseDatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsedDatabaseURL.Query()
	query.Set("search_path", schema)
	parsedDatabaseURL.RawQuery = query.Encode()
	databaseURL := parsedDatabaseURL.String()

	options := DefaultPostgresTranscriptStoreOptions()
	options.MaxEventsPerRun = 2
	options.MaxReplayEvents = 2
	options.PollInterval = 5 * time.Millisecond
	store, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	t.Run("concurrent startup serializes migrations", func(t *testing.T) {
		migrationSchema := fmt.Sprintf("transcript_migration_test_%d", time.Now().UnixNano())
		if _, err := pool.Exec(ctx, "CREATE SCHEMA "+migrationSchema); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA "+migrationSchema+" CASCADE") }()
		migrationURL := *parsedDatabaseURL
		migrationQuery := migrationURL.Query()
		migrationQuery.Set("search_path", migrationSchema)
		migrationURL.RawQuery = migrationQuery.Encode()

		const constructors = 8
		stores := make(chan *PostgresTranscriptStore, constructors)
		errorsCh := make(chan error, constructors)
		var wait sync.WaitGroup
		for range constructors {
			wait.Add(1)
			go func() {
				defer wait.Done()
				candidate, err := NewPostgresTranscriptStore(ctx, migrationURL.String(), options)
				if err != nil {
					errorsCh <- err
					return
				}
				stores <- candidate
			}()
		}
		wait.Wait()
		close(stores)
		close(errorsCh)
		for candidate := range stores {
			candidate.Close()
		}
		for err := range errorsCh {
			t.Fatal(err)
		}
	})

	t.Run("durable idempotency cursor and UID fencing", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "run-uid-1"}
		input := AppendTranscriptInput{Source: "adapter", IdempotencyKey: "event-1", Type: "output", Data: json.RawMessage(`{"text":"same"}`)}
		first, err := store.Append(ctx, run, input)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.Close()
		retry, err := restarted.Append(ctx, run, input)
		if err != nil {
			t.Fatal(err)
		}
		if !retry.Replayed || retry.Event.ID != first.Event.ID || retry.Event.Sequence != 1 {
			t.Fatalf("durable retry = %#v, want original event", retry)
		}
		subscription, err := restarted.Subscribe(ctx, run, first.Event.ID)
		if err != nil {
			t.Fatalf("cursor did not survive store restart: %v", err)
		}
		subscription.Unsubscribe()
		conflict := input
		conflict.Data = json.RawMessage(`{"text":"changed"}`)
		if _, err := restarted.Append(ctx, run, conflict); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflicting durable retry = %v", err)
		}
		otherRun := RunIdentity{Namespace: run.Namespace, UID: "run-uid-2"}
		other, err := restarted.Append(ctx, otherRun, input)
		if err != nil || other.Event.Sequence != 1 || other.Replayed {
			t.Fatalf("new Run UID append = %#v, %v", other, err)
		}
		if _, err := restarted.Subscribe(ctx, otherRun, first.Event.ID); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("cross-UID cursor error = %v, want invalid cursor", err)
		}
	})

	t.Run("opaque bytes and uint64 source sequence round trip", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "opaque-run"}
		maximum := ^uint64(0)
		input := AppendTranscriptInput{Source: "a\x00b", IdempotencyKey: "c\x00d", SourceSequence: &maximum, Type: "output\x00chunk", Data: json.RawMessage(`{"x":1}`)}
		result, err := store.Append(ctx, run, input)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := store.Append(ctx, run, input)
		if err != nil || !retry.Replayed || retry.Event.Source != input.Source || retry.Event.Type != input.Type || retry.Event.SourceSequence == nil || *retry.Event.SourceSequence != maximum {
			t.Fatalf("opaque retry = %#v, %v", retry, err)
		}
		byteDifferent := input
		byteDifferent.Data = json.RawMessage(`{ "x":1 }`)
		if _, err := store.Append(ctx, run, byteDifferent); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("byte-different retry error = %v", err)
		}
		if result.Event.ID != retry.Event.ID {
			t.Fatalf("opaque retry cursor changed: %q != %q", result.Event.ID, retry.Event.ID)
		}
	})

	t.Run("retained idempotency precedes a lowered capacity limit", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "changed-limit-run"}
		input := AppendTranscriptInput{Source: "adapter", IdempotencyKey: "large", Type: "output", Data: json.RawMessage(`{"payload":"retained"}`)}
		first, err := store.Append(ctx, run, input)
		if err != nil {
			t.Fatal(err)
		}
		lowered := options
		lowered.MaxBytesPerRun = transcriptEventSize(input) - 1
		limitedStore, err := NewPostgresTranscriptStore(ctx, databaseURL, lowered)
		if err != nil {
			t.Fatal(err)
		}
		defer limitedStore.Close()
		retry, err := limitedStore.Append(ctx, run, input)
		if err != nil || !retry.Replayed || retry.Event.ID != first.Event.ID {
			t.Fatalf("retry after lowered limit = %#v, %v", retry, err)
		}
		conflict := input
		conflict.Type = "changed"
		if _, err := limitedStore.Append(ctx, run, conflict); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflict after lowered limit = %v", err)
		}
	})

	t.Run("concurrent sequence allocation is contiguous", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "concurrent-run"}
		const count = 32
		sequences := make(chan uint64, count)
		errorsCh := make(chan error, count)
		var wait sync.WaitGroup
		for index := range count {
			wait.Add(1)
			go func() {
				defer wait.Done()
				result, err := store.Append(ctx, run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: fmt.Sprintf("event-%d", index), Type: "output", Data: json.RawMessage(`{}`)})
				if err != nil {
					errorsCh <- err
					return
				}
				sequences <- result.Event.Sequence
			}()
		}
		wait.Wait()
		close(sequences)
		close(errorsCh)
		for err := range errorsCh {
			t.Fatal(err)
		}
		got := make([]int, 0, count)
		for sequence := range sequences {
			got = append(got, int(sequence))
		}
		sort.Ints(got)
		for index, sequence := range got {
			if sequence != index+1 {
				t.Fatalf("sequence[%d] = %d", index, sequence)
			}
		}
	})

	t.Run("independent stores serialize the same idempotency key", func(t *testing.T) {
		peer, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		run := RunIdentity{Namespace: "project-a", UID: "cross-store-idempotency"}
		input := AppendTranscriptInput{Source: "adapter", IdempotencyKey: "same", Type: "output", Data: json.RawMessage(`{}`)}
		results := make(chan AppendTranscriptResult, 2)
		errorsCh := make(chan error, 2)
		var wait sync.WaitGroup
		for _, candidate := range []TranscriptStore{store, peer} {
			wait.Add(1)
			go func() {
				defer wait.Done()
				result, err := candidate.Append(ctx, run, input)
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
		created, replayed := 0, 0
		var eventID string
		for result := range results {
			if result.Replayed {
				replayed++
			} else {
				created++
			}
			if eventID != "" && eventID != result.Event.ID {
				t.Fatalf("cross-store event IDs differ: %q != %q", eventID, result.Event.ID)
			}
			eventID = result.Event.ID
		}
		if created != 1 || replayed != 1 {
			t.Fatalf("created/replayed = %d/%d", created, replayed)
		}
	})

	t.Run("bounded retention reports a recoverable gap", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "retention-run"}
		first := appendStoreEvent(t, store, run, "first")
		appendStoreEvent(t, store, run, "second")
		appendStoreEvent(t, store, run, "third")
		appendStoreEvent(t, store, run, "fourth")
		if _, err := store.Subscribe(ctx, run, first.Event.ID); !errors.Is(err, ErrExpiredCursor) {
			t.Fatalf("expired cursor error = %v", err)
		}
		subscription, err := store.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		if subscription.Gap == nil || subscription.Gap.EarliestSequence != 3 || len(subscription.History) != 2 {
			t.Fatalf("gap/history = %#v/%#v", subscription.Gap, subscription.History)
		}
		reused := appendStoreEvent(t, store, run, "first")
		if reused.Replayed || reused.Event.Sequence != 5 {
			t.Fatalf("evicted idempotency key = %#v", reused)
		}
	})

	t.Run("database polling closes replay live cut across stores", func(t *testing.T) {
		consumer, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		run := RunIdentity{Namespace: "project-a", UID: "live-run"}
		subscription, err := consumer.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		appended := appendStoreEvent(t, store, run, "live")
		select {
		case event := <-subscription.Events:
			if event.ID != appended.Event.ID || event.Sequence != 1 {
				t.Fatalf("live event = %#v, want %#v", event, appended.Event)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("database-polled live event was not delivered")
		}
	})

	t.Run("concurrent replay live cut delivers exactly once", func(t *testing.T) {
		wide := options
		wide.MaxEventsPerRun = 100
		wide.MaxReplayEvents = 100
		consumer, err := NewPostgresTranscriptStore(ctx, databaseURL, wide)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		producer, err := NewPostgresTranscriptStore(ctx, databaseURL, wide)
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()

		for iteration := range 20 {
			run := RunIdentity{Namespace: "project-a", UID: types.UID(fmt.Sprintf("cut-run-%d", iteration))}
			appendStoreEvent(t, producer, run, "before")
			start := make(chan struct{})
			subscriptions := make(chan TranscriptSubscription, 1)
			errorsCh := make(chan error, 2)
			go func() {
				<-start
				subscription, err := consumer.Subscribe(ctx, run, "")
				if err != nil {
					errorsCh <- err
					return
				}
				subscriptions <- subscription
			}()
			go func() {
				<-start
				_, err := producer.Append(ctx, run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "racing", Type: "output", Data: json.RawMessage(`{}`)})
				if err != nil {
					errorsCh <- err
				}
			}()
			close(start)

			var subscription TranscriptSubscription
			select {
			case err := <-errorsCh:
				t.Fatal(err)
			case subscription = <-subscriptions:
			}
			seen := make(map[uint64]int)
			for _, event := range subscription.History {
				seen[event.Sequence]++
			}
			deadline := time.After(2 * time.Second)
			for len(seen) < 2 {
				select {
				case event := <-subscription.Events:
					seen[event.Sequence]++
				case err := <-errorsCh:
					subscription.Unsubscribe()
					t.Fatal(err)
				case <-deadline:
					subscription.Unsubscribe()
					t.Fatalf("iteration %d saw sequences %#v", iteration, seen)
				}
			}
			subscription.Unsubscribe()
			if seen[1] != 1 || seen[2] != 1 {
				t.Fatalf("iteration %d saw sequences %#v", iteration, seen)
			}
		}
	})

	t.Run("polling drains multiple bounded batches", func(t *testing.T) {
		batched := options
		batched.MaxEventsPerRun = 100
		batched.MaxReplayEvents = 100
		batched.SubscriberBuffer = 2
		producer, err := NewPostgresTranscriptStore(ctx, databaseURL, batched)
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		consumer, err := NewPostgresTranscriptStore(ctx, databaseURL, batched)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		run := RunIdentity{Namespace: "project-a", UID: "poll-batches-run"}
		subscription, err := consumer.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		producerDone := make(chan error, 1)
		go func() {
			for index := range 7 {
				_, err := producer.Append(ctx, run, AppendTranscriptInput{
					Source: "adapter", IdempotencyKey: fmt.Sprintf("batch-%d", index),
					Type: "output", Data: json.RawMessage(`{"text":"value"}`),
				})
				if err != nil {
					producerDone <- err
					return
				}
			}
			producerDone <- nil
		}()
		for want := uint64(1); want <= 7; want++ {
			select {
			case event := <-subscription.Events:
				if event.Sequence != want {
					t.Fatalf("sequence = %d, want %d", event.Sequence, want)
				}
			case <-subscription.Dropped:
				t.Fatal("subscriber dropped while draining polling batches")
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for sequence %d", want)
			}
		}
		if err := <-producerDone; err != nil {
			t.Fatalf("produce polling batch: %v", err)
		}
	})

	t.Run("retention overtake drops the subscriber", func(t *testing.T) {
		overtaking := options
		overtaking.MaxEventsPerRun = 2
		overtaking.PollInterval = 250 * time.Millisecond
		producer, err := NewPostgresTranscriptStore(ctx, databaseURL, overtaking)
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		consumer, err := NewPostgresTranscriptStore(ctx, databaseURL, overtaking)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		run := RunIdentity{Namespace: "project-a", UID: "retention-overtake-run"}
		subscription, err := consumer.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		for index := range 4 {
			appendStoreEvent(t, producer, run, fmt.Sprintf("overtake-%d", index))
		}
		select {
		case <-subscription.Dropped:
		case <-time.After(2 * time.Second):
			t.Fatal("retention overtake did not drop subscriber")
		}
	})

	t.Run("subscriber buffer overrun drops the subscriber", func(t *testing.T) {
		overrun := options
		overrun.MaxEventsPerRun = 100
		overrun.MaxReplayEvents = 100
		overrun.SubscriberBuffer = 1
		producer, err := NewPostgresTranscriptStore(ctx, databaseURL, overrun)
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		consumer, err := NewPostgresTranscriptStore(ctx, databaseURL, overrun)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		run := RunIdentity{Namespace: "project-a", UID: "subscriber-overrun-run"}
		subscription, err := consumer.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		for index := range 3 {
			appendStoreEvent(t, producer, run, fmt.Sprintf("overrun-%d", index))
		}
		select {
		case <-subscription.Dropped:
		case <-time.After(2 * time.Second):
			t.Fatal("subscriber buffer overrun did not drop subscriber")
		}
	})

	t.Run("unsubscribe releases subscriber capacity", func(t *testing.T) {
		limited := options
		limited.MaxSubscribers = 1
		candidate, err := NewPostgresTranscriptStore(ctx, databaseURL, limited)
		if err != nil {
			t.Fatal(err)
		}
		defer candidate.Close()
		run := RunIdentity{Namespace: "project-a", UID: "subscriber-lifecycle-run"}
		first, err := candidate.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := candidate.Subscribe(ctx, run, ""); !errors.Is(err, ErrSubscriberCapacity) {
			first.Unsubscribe()
			t.Fatalf("second subscriber error = %v", err)
		}
		first.Unsubscribe()
		first.Unsubscribe()
		replacement, err := candidate.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatalf("subscribe after unsubscribe: %v", err)
		}
		replacement.Unsubscribe()
	})

	t.Run("database failure is never acknowledged", func(t *testing.T) {
		failed, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		failed.Close()
		result, err := failed.Append(ctx, RunIdentity{Namespace: "project-a", UID: "failed-run"}, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "failed", Type: "output", Data: json.RawMessage(`{}`)})
		if err == nil || result.Event.Sequence != 0 {
			t.Fatalf("failed append result/error = %#v/%v", result, err)
		}
	})

	t.Run("poll failure drops the subscription", func(t *testing.T) {
		failed, err := NewPostgresTranscriptStore(ctx, databaseURL, options)
		if err != nil {
			t.Fatal(err)
		}
		subscription, err := failed.Subscribe(ctx, RunIdentity{Namespace: "project-a", UID: "poll-failure-run"}, "")
		if err != nil {
			failed.Close()
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		failed.Close()
		select {
		case <-subscription.Dropped:
		case <-time.After(time.Second):
			t.Fatal("database polling failure did not drop subscription")
		}
	})

	t.Run("signed bigint ceiling remains replayable", func(t *testing.T) {
		run := RunIdentity{Namespace: "project-a", UID: "sequence-ceiling-run"}
		createdAt := time.Now().UTC()
		_, err := store.pool.Exec(ctx, `INSERT INTO transcript_runs(namespace, run_uid, high_water, retained_events, retained_bytes) VALUES ($1,$2,$3,1,256)`, run.Namespace, string(run.UID), int64(maxPostgresTranscriptSequence))
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.pool.Exec(ctx, `INSERT INTO transcript_events(namespace, run_uid, sequence, source, idempotency_key, event_type, data, created_at, event_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,256)`, run.Namespace, string(run.UID), int64(maxPostgresTranscriptSequence), []byte("adapter"), []byte("last"), []byte("output"), []byte(`{}`), createdAt)
		if err != nil {
			t.Fatal(err)
		}
		subscription, err := store.Subscribe(ctx, run, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(subscription.History) != 1 || subscription.History[0].Sequence != maxPostgresTranscriptSequence {
			t.Fatalf("ceiling history = %#v", subscription.History)
		}
		subscription.Unsubscribe()
		if _, err := store.Append(ctx, run, AppendTranscriptInput{Source: "adapter", IdempotencyKey: "overflow", Type: "output", Data: json.RawMessage(`{}`)}); !errors.Is(err, ErrTranscriptCapacity) {
			t.Fatalf("ceiling append error = %v", err)
		}
	})
}
