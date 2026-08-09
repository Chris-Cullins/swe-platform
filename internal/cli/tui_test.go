package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	tea "github.com/charmbracelet/bubbletea"
)

func TestTUIModelListDetailCancelAndFreeFormCreate(t *testing.T) {
	client, _ := controlplaneclient.New("http://control.test", "secret", &http.Client{})
	model := newTUIModel(context.Background(), client, "team-a")
	run := controlplane.Run{
		Name: "run-one", UID: "uid-one", State: "Running", CreatedAt: time.Now(),
		Intent: controlplane.RunIntent{Agent: "agent-added-next-year", Prompt: "do work", Selector: controlplane.RunSelector{Template: "small"}},
	}
	_, _ = model.Update(runsLoadedMsg{snapshot: controlplaneclient.RunSummarySnapshot{Items: []controlplane.RunSummary{{Name: run.Name, UID: run.UID, Agent: run.Intent.Agent, PromptPreview: run.Intent.Prompt, State: run.State}}}})
	if len(model.runs) != 1 || model.runs[0].Agent != "agent-added-next-year" {
		t.Fatalf("runs = %#v", model.runs)
	}
	_, command := model.Update(keyMessage("enter"))
	if model.mode != tuiDetail || model.run == nil || command == nil {
		t.Fatalf("detail state = mode %d, run %#v, command %v", model.mode, model.run, command)
	}
	_, _ = model.Update(keyMessage("x"))
	if model.mode != tuiConfirmCancel {
		t.Fatalf("cancel did not require confirmation: mode %d", model.mode)
	}
	_, _ = model.Update(keyMessage("n"))
	if model.mode != tuiDetail {
		t.Fatalf("cancel rejection mode = %d", model.mode)
	}

	model.mode = tuiCreate
	values := []string{"stable-name", "agent-added-next-year", "", "project-a", "small", "profile-a"}
	for i := range values {
		model.fields[i].SetValue(values[i])
	}
	model.prompt.SetValue("opaque prompt")
	request := model.createRequest()
	if request.Name != "stable-name" || request.Agent != "agent-added-next-year" || request.Selector.Project != "project-a" || request.Selector.Template != "small" || request.Prompt != "opaque prompt" {
		t.Fatalf("create request = %#v", request)
	}
}

func TestTUIAllowsOnlyOneCreateMutationAtATime(t *testing.T) {
	client, _ := controlplaneclient.New("http://control.test", "token", &http.Client{})
	model := newTUIModel(context.Background(), client, "team-a")
	model.mode = tuiCreate
	model.fields[0].SetValue("stable")
	model.fields[1].SetValue("future-agent")
	model.fields[4].SetValue("small")
	model.prompt.SetValue("task")
	_, first := model.Update(keyMessage("ctrl+s"))
	_, second := model.Update(keyMessage("ctrl+s"))
	before := model.fields[0].Value()
	_, edit := model.Update(keyMessage("z"))
	if first == nil || second != nil || edit != nil || !model.mutationInFlight || model.fields[0].Value() != before {
		t.Fatalf("mutation guard = first %v, second %v, edit %v, inflight %t, name %q", first, second, edit, model.mutationInFlight, model.fields[0].Value())
	}
}

func TestTUIModelFencesTranscriptByImmutableRunIdentity(t *testing.T) {
	client, _ := controlplaneclient.New("http://control.test", "token", &http.Client{})
	model := newTUIModel(context.Background(), client, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same-name", UID: "old-uid"}
	model.streamID = runIdentity{namespace: "team-a", name: "same-name", uid: "old-uid"}
	model.streamCursor = "old-cursor"
	model.transcript = []string{"old event"}
	cancelled := false
	model.streamCancel = func() { cancelled = true }

	replacement := controlplane.Run{Name: "same-name", UID: "new-uid", State: "Running"}
	_, command := model.Update(runLoadedMsg{name: "same-name", run: replacement})
	if !cancelled || model.streamID.uid != "new-uid" || model.streamCursor != "" || len(model.transcript) != 0 || command == nil {
		t.Fatalf("replacement state = cancelled %t, identity %#v, cursor %q, transcript %#v", cancelled, model.streamID, model.streamCursor, model.transcript)
	}
	_, _ = model.Update(transcriptDoneMsg{identity: model.streamID, generation: 0, err: context.Canceled})
	if model.streamCancel == nil {
		t.Fatal("stale completion cleared the replacement stream")
	}

	_, _ = model.Update(transcriptMsg{
		identity:   runIdentity{namespace: "team-a", name: "same-name", uid: "old-uid"},
		generation: 0,
		event:      controlplaneclient.SSEEvent{ID: "old-event", Event: "transcript", Data: []byte(`{"source":"adapter","type":"output","data":{}}`)},
	})
	if len(model.transcript) != 0 {
		t.Fatalf("stale transcript was rendered: %#v", model.transcript)
	}
	model.stopStream()
}

func TestTUIReplacementEventCoalescesExactDetailRefresh(t *testing.T) {
	client, _ := controlplaneclient.New("http://control.test", "token", &http.Client{})
	model := newTUIModel(context.Background(), client, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same", UID: "old", Generation: 1}
	model.runs = []controlplane.RunSummary{{Name: "same", UID: "old", Generation: 1}}
	model.resourceGeneration = 2
	model.detailInFlight = true
	model.streamBlocked = true
	committed := make(chan struct{})
	_, command := model.Update(runWatchMsg{generation: 2, event: controlplane.RunWatchEvent{Type: "ADDED", ResourceVersion: "2", Run: controlplane.RunSummary{Name: "same", UID: "new", Generation: 1}}, committed: committed})
	if command == nil || !model.detailRefreshPending || len(model.runs) != 1 || model.runs[0].UID != "new" {
		t.Fatalf("replacement state = pending %t, runs %#v, command %v", model.detailRefreshPending, model.runs, command)
	}
	select {
	case <-committed:
	default:
		t.Fatal("replacement event was not committed after handling")
	}
	_, refresh := model.Update(runLoadedMsg{name: "same", run: *model.run})
	if refresh == nil || !model.detailInFlight || model.detailRefreshPending {
		t.Fatalf("coalesced refresh = %v, inflight %t, pending %t", refresh, model.detailInFlight, model.detailRefreshPending)
	}
}

func TestTUIPollRecoversSnapshotAndExactDetailFailures(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.listInFlight = true
	_, _ = model.Update(runsLoadedMsg{err: errors.New("temporary list failure")})
	if model.resourceCancel != nil || model.listInFlight || !model.listNeedsRefresh {
		t.Fatalf("failed snapshot state = cancel %v, inflight %t, needs refresh %t", model.resourceCancel, model.listInFlight, model.listNeedsRefresh)
	}
	_, command := model.Update(pollMsg(time.Now()))
	if command == nil || !model.listInFlight {
		t.Fatalf("snapshot recovery = command %v, inflight %t", command, model.listInFlight)
	}

	model.listInFlight = false
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same", UID: "uid"}
	model.detailInFlight = true
	_, _ = model.Update(runLoadedMsg{name: "same", expectedUID: "uid", err: &url.Error{Op: "Get", URL: "https://control-plane", Err: errors.New("temporary detail failure")}})
	if !model.detailNeedsRefresh || model.detailInFlight {
		t.Fatalf("failed detail state = needs refresh %t, inflight %t", model.detailNeedsRefresh, model.detailInFlight)
	}
	_, command = model.Update(pollMsg(time.Now()))
	if command == nil || !model.detailInFlight {
		t.Fatalf("detail recovery = command %v, inflight %t", command, model.detailInFlight)
	}
}

func TestTUIExactDetailTerminalFailuresRemainBlockedUntilExplicitSelection(t *testing.T) {
	terminalErrors := []error{
		&controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: http.StatusNotFound}},
		&controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: http.StatusConflict}},
		fmt.Errorf("exact GET: %w", controlplaneclient.ErrRunIdentityMismatch),
	}
	for _, terminalErr := range terminalErrors {
		model := newTUIModel(context.Background(), nil, "team-a")
		model.mode = tuiDetail
		model.run = &controlplane.Run{Name: "same", UID: "selected-uid", Environment: &controlplane.RunEnvironment{Name: "old-environment"}}
		model.pollFallback = true
		model.detailInFlight = true
		_, command := model.Update(runLoadedMsg{name: "same", expectedUID: "selected-uid", err: terminalErr})
		if command != nil || !model.detailRefreshBlocked || model.detailNeedsRefresh || model.detailInFlight {
			t.Fatalf("terminal state for %v = command %v, blocked %t, needs refresh %t, inflight %t", terminalErr, command, model.detailRefreshBlocked, model.detailNeedsRefresh, model.detailInFlight)
		}
		_, _ = model.Update(pollMsg(time.Now()))
		if model.detailInFlight || model.envInFlight {
			t.Fatalf("fallback poll restarted blocked detail children for %v: detail %t, environment %t", terminalErr, model.detailInFlight, model.envInFlight)
		}
		_, _ = model.Update(keyMessage("r"))
		if model.detailInFlight {
			t.Fatalf("manual refresh restarted blocked selection for %v", terminalErr)
		}
		model.listInFlight = false
		model.mode = tuiList
		model.runs = []controlplane.RunSummary{{Name: "same", UID: "replacement-uid"}}
		_, command = model.Update(keyMessage("enter"))
		if command == nil || model.detailRefreshBlocked || !model.detailInFlight || model.run.UID != "replacement-uid" {
			t.Fatalf("explicit selection state for %v = command %v, blocked %t, inflight %t, run %#v", terminalErr, command, model.detailRefreshBlocked, model.detailInFlight, model.run)
		}
	}
}

func TestTUIExactDetailTransientStatusesContinueRecovery(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		model := newTUIModel(context.Background(), nil, "team-a")
		model.mode = tuiDetail
		model.run = &controlplane.Run{Name: "run", UID: "uid"}
		model.detailInFlight = true
		err := &controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: status}}
		_, _ = model.Update(runLoadedMsg{name: "run", expectedUID: "uid", err: err})
		_, command := model.Update(pollMsg(time.Now()))
		if model.detailRefreshBlocked || command == nil || !model.detailInFlight {
			t.Fatalf("status %d recovery = blocked %t, command %v, inflight %t", status, model.detailRefreshBlocked, command, model.detailInFlight)
		}
	}
}

func TestTUIEnvironmentMismatchAndNotFoundStopOnlyEnvironmentPolling(t *testing.T) {
	for _, terminalErr := range []error{
		fmt.Errorf("exact Environment GET: %w", controlplaneclient.ErrEnvironmentIdentityMismatch),
		&controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: http.StatusNotFound}},
	} {
		model := newTUIModel(context.Background(), nil, "team-a")
		model.mode = tuiDetail
		model.pollFallback = true
		model.run = &controlplane.Run{Name: "run", UID: "run-uid", Environment: &controlplane.RunEnvironment{Name: "env", UID: "env-uid"}}
		model.env = &controlplane.Environment{Name: "env", UID: "env-uid"}
		model.envInFlight = true
		_, _ = model.Update(environmentLoadedMsg{identity: model.currentIdentity(), name: "env", expectedUID: "env-uid", err: terminalErr})
		if !model.envRefreshBlocked || model.env != nil || model.run == nil {
			t.Fatalf("terminal Environment state for %v = blocked %t, env %#v, run %#v", terminalErr, model.envRefreshBlocked, model.env, model.run)
		}
		_, _ = model.Update(pollMsg(time.Now()))
		if model.envInFlight {
			t.Fatalf("terminal Environment error %v restarted polling", terminalErr)
		}
	}
}

func TestTUIEnvironmentTransient503RecoversOnPoll(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "run", UID: "run-uid", Environment: &controlplane.RunEnvironment{Name: "env", UID: "env-uid"}}
	model.envInFlight = true
	err := &controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: http.StatusServiceUnavailable}}
	_, _ = model.Update(environmentLoadedMsg{identity: model.currentIdentity(), name: "env", expectedUID: "env-uid", err: err})
	_, command := model.Update(pollMsg(time.Now()))
	if model.envRefreshBlocked || command == nil || !model.envInFlight {
		t.Fatalf("503 recovery = blocked %t, command %v, inflight %t", model.envRefreshBlocked, command, model.envInFlight)
	}
}

func TestTUIEnvironmentAssociationChangeClearsTerminalBlock(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "run", UID: "run-uid", Environment: &controlplane.RunEnvironment{Name: "env", UID: "old-uid"}}
	model.envRefreshBlocked = true
	model.env = &controlplane.Environment{Name: "env", UID: "old-uid"}
	replacement := controlplane.Run{Name: "run", UID: "run-uid", Environment: &controlplane.RunEnvironment{Name: "env", UID: "new-uid"}}
	_, command := model.Update(runLoadedMsg{name: "run", expectedUID: "run-uid", run: replacement})
	if model.envRefreshBlocked || model.env != nil || command == nil || !model.envInFlight {
		t.Fatalf("association change = blocked %t, env %#v, command %v, inflight %t", model.envRefreshBlocked, model.env, command, model.envInFlight)
	}
}

func TestTUIWatchDeleteReconcilesOnlySelectedUID(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same", UID: "replacement-b"}
	model.runs = []controlplane.RunSummary{{Name: "same", UID: "replacement-b"}}
	model.resourceGeneration = 1
	staleCommitted := make(chan struct{})
	_, command := model.Update(runWatchMsg{generation: 1, event: controlplane.RunWatchEvent{Type: "DELETED", ResourceVersion: "2", Run: controlplane.RunSummary{Name: "same", UID: "old-a"}}, committed: staleCommitted})
	if model.detailInFlight || model.detailRefreshPending {
		t.Fatalf("stale delete refreshed replacement: command %v, inflight %t, pending %t", command, model.detailInFlight, model.detailRefreshPending)
	}

	model.run = &controlplane.Run{Name: "same", UID: "old-a"}
	currentCommitted := make(chan struct{})
	_, command = model.Update(runWatchMsg{generation: 1, event: controlplane.RunWatchEvent{Type: "DELETED", ResourceVersion: "3", Run: controlplane.RunSummary{Name: "same", UID: "old-a"}}, committed: currentCommitted})
	if command == nil || !model.detailInFlight {
		t.Fatalf("current delete did not settle exact identity once: command %v, inflight %t", command, model.detailInFlight)
	}
	notFound := &controlplaneclient.ProblemError{Problem: controlplaneclient.Problem{Status: http.StatusNotFound}}
	_, command = model.Update(runLoadedMsg{name: "same", expectedUID: "old-a", err: notFound})
	if command != nil || !model.detailRefreshBlocked || model.detailInFlight {
		t.Fatalf("current delete settlement = command %v, blocked %t, inflight %t", command, model.detailRefreshBlocked, model.detailInFlight)
	}
	_, _ = model.Update(pollMsg(time.Now()))
	if model.detailInFlight {
		t.Fatal("settled current delete entered a refresh loop")
	}
}

func TestTUIWatchErrorRecoversThroughSnapshotWithoutCompatibilityFallback(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.resourceGeneration = 3
	model.resourceCancel = func() {}
	model.resourceDone = make(chan struct{})

	_, command := model.Update(runWatchDoneMsg{generation: 3, err: errors.New("invalid watch event")})
	if command != nil || model.resourceCancel != nil || model.resourceDone != nil || !model.listNeedsRefresh || model.pollFallback {
		t.Fatalf("watch error recovery state = command %v, cancel %v, done %v, needs refresh %t, fallback %t", command, model.resourceCancel, model.resourceDone, model.listNeedsRefresh, model.pollFallback)
	}
	_, command = model.Update(pollMsg(time.Now()))
	if command == nil || !model.listInFlight || model.pollFallback {
		t.Fatalf("snapshot retry state = command %v, inflight %t, fallback %t", command, model.listInFlight, model.pollFallback)
	}
}

func TestTUIDiscardedDetailLoadImmediatelyRefreshesCurrentSelection(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same", UID: "selected-uid"}
	model.detailInFlight = true
	_, command := model.Update(runLoadedMsg{
		name: "same", expectedUID: "old-uid", run: controlplane.Run{Name: "same", UID: "old-uid"},
	})
	if command == nil || !model.detailInFlight || !model.detailNeedsRefresh {
		t.Fatalf("discard recovery = command %v, inflight %t, needs refresh %t", command, model.detailInFlight, model.detailNeedsRefresh)
	}
}

func TestTUICancellationGenerationDoesNotRestartTranscript(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "run", UID: "uid", Generation: 1}
	model.streamID = runIdentity{namespace: "team-a", name: "run", uid: "uid"}
	model.streamCursor = "cursor"
	model.transcript = []string{"retained"}
	cancelled := false
	model.streamCancel = func() { cancelled = true }
	_, command := model.Update(runLoadedMsg{name: "run", expectedUID: "uid", run: controlplane.Run{Name: "run", UID: "uid", Generation: 2, CancelRequested: true}})
	if cancelled || command != nil || model.streamCursor != "cursor" || len(model.transcript) != 1 || model.run.Generation != 2 {
		t.Fatalf("generation update = cancelled %t, command %v, cursor %q, transcript %#v, run %#v", cancelled, command, model.streamCursor, model.transcript, model.run)
	}
}

func TestTUITranscriptPumpRearmsIgnoredLiveMessagesOnly(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	current := runIdentity{namespace: "team-a", name: "run", uid: "uid"}
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "run", UID: "uid"}
	model.streamID = current
	model.streamGeneration = 3
	model.transcriptMessages = make(chan tea.Msg)
	_, command := model.Update(transcriptMsg{identity: runIdentity{namespace: "team-a", name: "other", uid: "other"}, generation: 3})
	if command == nil {
		t.Fatal("ignored live-generation message did not rearm the transcript reader")
	}
	_, command = model.Update(transcriptMsg{identity: current, generation: 2})
	if command != nil {
		t.Fatal("stale stream generation rearmed the current transcript reader")
	}
}

func TestTUITranscriptRenderingIsGapVisibleSafeAndBounded(t *testing.T) {
	event := controlplaneclient.SSEEvent{
		ID:    "cursor",
		Event: "transcript",
		Data:  []byte(`{"source":"future\u001b]8;;bad","type":"custom\nkind","data":{"opaque":"\u001b[31mred"}}`),
	}
	rendered := formatTranscriptEvent(event)
	if strings.ContainsRune(rendered, '\x1b') || strings.ContainsRune(rendered, '\n') || !strings.Contains(rendered, "future") || !strings.Contains(rendered, `\u001b[31mred`) {
		t.Fatalf("unsafe or incorrect transcript rendering = %q", rendered)
	}
	gap := formatTranscriptEvent(controlplaneclient.SSEEvent{Event: "transcript-gap", Data: []byte(`{"dropped":7}`)})
	if !strings.Contains(gap, "TRANSCRIPT GAP") || !strings.Contains(gap, "dropped") {
		t.Fatalf("gap rendering = %q", gap)
	}
	model := &tuiModel{}
	for i := 0; i < maxTranscriptEntries+20; i++ {
		model.appendTranscript(fmt.Sprint(i))
	}
	if len(model.transcript) != maxTranscriptEntries || model.transcript[0] != "20" {
		t.Fatalf("bounded transcript = len %d, first %q", len(model.transcript), model.transcript[0])
	}
}

func TestTUIResumesAtServerCursorAndMakesRetentionLossVisible(t *testing.T) {
	client, _ := controlplaneclient.New("http://control.test", "token", &http.Client{})
	model := newTUIModel(context.Background(), client, "team-a")
	identity := runIdentity{namespace: "team-a", name: "run-one", uid: "uid-one"}
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: identity.name, UID: identity.uid}
	model.streamID = identity
	model.streamCursor = "expired"
	problem := &controlplaneclient.ProblemError{
		Status:  "410 Gone",
		Problem: controlplaneclient.Problem{Type: "https://swe-platform.dev/problems/cursor_expired", Status: http.StatusGone},
		Body:    []byte(`{"resumeAfter":"retained-boundary","available":{"earliestSequence":8,"latestSequence":12}}`),
	}
	_, command := model.Update(transcriptDoneMsg{identity: identity, generation: model.streamGeneration, err: problem})
	if command == nil || model.streamCursor != "retained-boundary" || len(model.transcript) != 1 || !strings.Contains(model.transcript[0], "TRANSCRIPT GAP") {
		t.Fatalf("recovery state = command %v, cursor %q, transcript %#v", command, model.streamCursor, model.transcript)
	}
	second := &controlplaneclient.ProblemError{
		Status:  "410 Gone",
		Problem: controlplaneclient.Problem{Type: "https://swe-platform.dev/problems/cursor_expired", Status: http.StatusGone},
		Body:    []byte(`{"resumeAfter":"newer-boundary","available":{"earliestSequence":20,"latestSequence":30}}`),
	}
	_, reconnect := model.Update(transcriptDoneMsg{identity: identity, generation: model.streamGeneration, err: second})
	if reconnect != nil || !model.streamBlocked || model.streamCancel != nil {
		t.Fatalf("second recovery was not blocked: command %v, blocked %t, cancel %v", reconnect, model.streamBlocked, model.streamCancel)
	}
	_, pollRestart := model.Update(runLoadedMsg{name: identity.name, run: *model.run})
	if model.streamCancel != nil || pollRestart != nil {
		t.Fatalf("poll restarted blocked recovery: cancel %v, command %v", model.streamCancel, pollRestart)
	}
	model.stopStream()
}

func TestTUIRendersGapAtCurrentCursorWithoutAdvancingIt(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	identity := runIdentity{namespace: "team-a", name: "run", uid: "uid"}
	model.mode, model.run, model.streamID = tuiDetail, &controlplane.Run{Name: "run", UID: "uid"}, identity
	model.streamCursor = "cursor"
	_, _ = model.Update(transcriptMsg{identity: identity, generation: model.streamGeneration, event: controlplaneclient.SSEEvent{ID: "cursor", Event: "transcript-gap", Data: []byte(`{"dropped":2}`)}})
	if model.streamCursor != "cursor" || len(model.transcript) != 1 || !strings.Contains(model.transcript[0], "TRANSCRIPT GAP") {
		t.Fatalf("cursor/transcript = %q/%#v", model.streamCursor, model.transcript)
	}
}

func TestTUIConfirmationCapturesImmutableRunUID(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.mode = tuiDetail
	model.run = &controlplane.Run{Name: "same", UID: "old", State: "Running"}
	_, _ = model.Update(keyMessage("x"))
	model.run = &controlplane.Run{Name: "same", UID: "replacement", State: "Running"}
	if model.cancelIdentity != (runIdentity{namespace: "team-a", name: "same", uid: "old"}) {
		t.Fatalf("captured identity = %#v", model.cancelIdentity)
	}
	_, command := model.Update(keyMessage("y"))
	if command == nil || model.cancelIdentity.uid != "old" {
		t.Fatalf("confirmation command/identity = %v/%#v", command, model.cancelIdentity)
	}
}

func TestTUIAdvertisesTerminalOnlyForExactEnvironmentIncarnation(t *testing.T) {
	model := newTUIModel(context.Background(), nil, "team-a")
	model.run = &controlplane.Run{Name: "run", UID: "run-uid", TerminalAvailable: true, Environment: &controlplane.RunEnvironment{Name: "env", UID: "env-uid"}}
	model.env = &controlplane.Environment{Name: "env", UID: "replacement-uid"}
	if model.canAttachTerminal() {
		t.Fatal("replacement Environment was attachable")
	}
	model.env.UID = "env-uid"
	if !model.canAttachTerminal() {
		t.Fatal("exact Environment association was not attachable")
	}
}

func TestTUITranscriptStreamStopsWithContext(t *testing.T) {
	connected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/transcript") || r.Header.Get("SWE-Run-UID") != "uid-one" {
			t.Errorf("transcript request path/UID = %q/%q", r.URL.Path, r.Header.Get("SWE-Run-UID"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(connected)
		<-r.Context().Done()
	}))
	defer server.Close()
	client, _ := controlplaneclient.New(server.URL, "token", server.Client())
	model := newTUIModel(context.Background(), client, "team-a")
	identity := runIdentity{namespace: "team-a", name: "run-one", uid: "uid-one"}
	command := model.startTranscript(identity)
	batch, ok := command().(tea.BatchMsg)
	if !ok || len(batch) != 2 {
		t.Fatalf("startTranscript command = %#v", batch)
	}
	result := make(chan tea.Msg, 1)
	go func() { result <- batch[0]() }()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("stream did not connect")
	}
	model.stopStream()
	select {
	case message := <-result:
		done, ok := message.(transcriptDoneMsg)
		if !ok || !errors.Is(done.err, context.Canceled) {
			t.Fatalf("stream result = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not stop after cancellation")
	}
}

func TestTUICheckUsesRunAPIAndNeverPrintsToken(t *testing.T) {
	const token = "check-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token || r.URL.Path != "/api/v1/namespaces/team-a/runs" {
			t.Fatalf("request auth/path = %q/%q", r.Header.Get("Authorization"), r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"items":[]}`)
	}))
	defer server.Close()
	command := NewRootCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"tui", "--control-plane", server.URL, "--token", token, "--check", "--namespace", "team-a"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "namespace team-a") || strings.Contains(output.String(), token) {
		t.Fatalf("check output = %q", output.String())
	}
}

func keyMessage(value string) tea.KeyMsg {
	switch value {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case "ctrl+s":
		return tea.KeyMsg{Type: tea.KeyCtrlS}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(value)}
}
