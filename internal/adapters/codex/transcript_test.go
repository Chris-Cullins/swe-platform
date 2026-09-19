package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// Fixtures follow rust-v0.144.6 exec/tests/event_processor_with_json_output.rs,
// including nullable exit_code and the five turn.completed usage fields.
const messageLine = `{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"hello 界🙂"}}`
const commandLine = `{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"ls","aggregated_output":"a.txt\n","exit_code":0,"status":"completed"}}`

func transcriptChunk(t *testing.T, f *TranscriptFormatter, execution, stream string, offset uint64, data string, eof bool, gap uint64) {
	t.Helper()
	chunk := outputEvent{ExecutionID: execution, Stream: stream, Offset: offset, NextOffset: offset + uint64(len(data)), ProducedEnd: offset + uint64(len(data)), GapBytes: gap, EOF: eof, Data: []byte(data)}
	body, err := json.Marshal(map[string]any{"source": "codex", "type": "codex.process-output", "data": chunk})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Write("transcript", body); err != nil {
		t.Fatal(err)
	}
}

func TestTranscriptSplitUTF8OverlapReplayAndEOF(t *testing.T) {
	for split := 1; split < len(messageLine); split++ {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			var out bytes.Buffer
			f := NewTranscriptFormatter(&out)
			transcriptChunk(t, f, "a", "stdout", 0, messageLine[:split], false, 0)
			// Asymmetric overlap must append only the unseen suffix.
			start := max(0, split-7)
			transcriptChunk(t, f, "a", "stdout", uint64(start), messageLine[start:], true, 0)
			transcriptChunk(t, f, "a", "stdout", 0, messageLine, true, 0)
			want := "[Codex execution (agent output)]\na\n[Codex agent-reported message]\nhello 界🙂\n"
			if out.String() != want {
				t.Fatalf("output=%q", out.String())
			}
		})
	}
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	transcriptChunk(t, f, "a", "stdout", 0, messageLine, false, 0)
	transcriptChunk(t, f, "a", "stdout", 0, messageLine, true, 0)
	if strings.Count(out.String(), "hello 界🙂") != 1 {
		t.Fatal(out.String())
	}
}

func TestTranscriptPinnedRecordsAndProvenance(t *testing.T) {
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	records := []string{`{"type":"thread.started","thread_id":"67e55044-10b1-426f-9247-bb680e5fe0c8"}`, `{"type":"turn.started"}`, commandLine,
		strings.ReplaceAll(strings.ReplaceAll(commandLine, `"completed"}`, `"declined"}`), `"exit_code":0`, `"exit_code":null`),
		`{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":3,"cache_write_input_tokens":4,"output_tokens":29,"reasoning_output_tokens":7}}`,
		`{"type":"turn.failed","error":{"message":"backend failed (request id abc)"}}`}
	transcriptChunk(t, f, "a", "stdout", 0, strings.Join(records, "\n")+"\n", true, 0)
	for _, want := range []string{"command: ls\nstatus: completed; exit_code: 0\na.txt", "status: declined; exit_code: null", "not platform verification", "usage is not accounting", "thread.started", "backend failed"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q: %s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "raw fallback") {
		t.Fatal(out.String())
	}
}

func TestTranscriptLossBoundaries(t *testing.T) {
	for _, boundary := range []string{"transcript-gap", "client-gap", "process", "execution", "invalid"} {
		t.Run(boundary, func(t *testing.T) {
			var out bytes.Buffer
			f := NewTranscriptFormatter(&out)
			split := len(messageLine) - 5
			transcriptChunk(t, f, "a", "stdout", 0, messageLine[:split], false, 0)
			execution := "a"
			offset := uint64(split)
			gap := uint64(0)
			switch boundary {
			case "process":
				offset += 3
				gap = 3
			case "execution":
				execution = "b"
				offset = 0
			case "invalid":
				if err := f.Write("transcript", []byte(`{"source":"codex","type":"codex.process-output","data":{"executionId":"a"}}`)); err != nil {
					t.Fatal(err)
				}
			default:
				if err := f.Write(boundary, []byte(`{"resumeAfter":"opaque"}`)); err != nil {
					t.Fatal(err)
				}
			}
			transcriptChunk(t, f, execution, "stdout", offset, messageLine[split:]+"\n"+commandLine+"\n", true, gap)
			if strings.Contains(out.String(), "[Codex agent-reported message]") || !strings.Contains(out.String(), "raw fallback") || !strings.Contains(out.String(), "command: ls") {
				t.Fatal(out.String())
			}
		})
	}
}

func TestTranscriptConflictRetiredExecutionAndStreams(t *testing.T) {
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	transcriptChunk(t, f, "a", "stdout", 0, messageLine[:20], false, 0)
	transcriptChunk(t, f, "a", "stderr", 0, "stderr 🙂\n", true, 0)
	transcriptChunk(t, f, "a", "stdout", 10, "DIFFERENT bytes", false, 0)
	transcriptChunk(t, f, "b", "stdout", 0, messageLine+"\n", true, 0)
	transcriptChunk(t, f, "a", "stdout", 0, messageLine+"\n", true, 0)
	for _, want := range []string{"stderr 🙂", "conflicting or unverifiable overlap", "retired execution replay"} {
		if !strings.Contains(out.String(), want) {
			t.Fatal(out.String())
		}
	}
	if strings.Count(out.String(), "[Codex agent-reported message]") != 1 {
		t.Fatal(out.String())
	}
}

func TestTranscriptBoundsFallbackAndResynchronization(t *testing.T) {
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	for i := 0; i < 6; i++ {
		transcriptChunk(t, f, "a", "stdout", uint64(i*pageMax), strings.Repeat("x", pageMax), false, 0)
	}
	transcriptChunk(t, f, "a", "stdout", 6*pageMax, "\n"+messageLine+"\n", true, 0)
	if !strings.Contains(out.String(), "oversized line") || !strings.Contains(out.String(), "output truncated") || !strings.Contains(out.String(), "hello 界🙂") {
		t.Fatal(out.String())
	}
	if out.Len() > 4*maxOutputBytes {
		t.Fatalf("unbounded output: %d", out.Len())
	}
	for _, s := range f.streams {
		if len(s.line) > maxLineBytes || len(s.tail) > maxLineBytes {
			t.Fatal("unbounded state")
		}
	}
	// Replay outside the bounded verification window cannot be silently trusted.
	transcriptChunk(t, f, "a", "stdout", 0, "xxxx", false, 0)
	if !strings.Contains(out.String(), "unverifiable overlap") {
		t.Fatal(out.String())
	}
	for i := 1; i <= maxExecutions; i++ {
		transcriptChunk(t, f, fmt.Sprint(i), "stdout", 0, "{}\n", true, 0)
	}
	if len(f.seen) != maxExecutions || !f.disabled || !strings.Contains(out.String(), "execution tracking limit") {
		t.Fatal("execution budget not enforced")
	}
}

func TestTranscriptUnknownMalformedSanitizedAndClose(t *testing.T) {
	for _, line := range []string{`{"type":"future","secret":"visible raw"}`, `{"type":"item.completed","item":{"id":"x","type":"agent_message"}}`, "not JSON", string([]byte{0xff}), `{"type":"item.completed","item":{"id":"x","type":"command_execution","command":"x","aggregated_output":"x","exit_code":"zero","status":"completed"}}`, `{"type":"turn.completed","usage":{"input_tokens":"many"}}`} {
		var out bytes.Buffer
		f := NewTranscriptFormatter(&out)
		transcriptChunk(t, f, "a", "stdout", 0, line+"\n", true, 0)
		if !strings.Contains(out.String(), "raw fallback") {
			t.Fatal(out.String())
		}
	}
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	text := "unsafe\x1b]52;c;encoded\a\r\u009b2J\u202e" + strings.Repeat("界", maxOutputBytes)
	line, _ := json.Marshal(map[string]any{"type": "item.completed", "item": map[string]any{"id": "x", "type": "agent_message", "text": text}})
	transcriptChunk(t, f, "a", "stdout", 0, string(line)+"\n", true, 0)
	if !utf8.Valid(out.Bytes()) || !strings.Contains(out.String(), "output truncated") || out.Len() > maxOutputBytes+1024 {
		t.Fatal("unsafe or unbounded output")
	}
	for _, r := range out.String() {
		if (unicode.IsControl(r) && r != '\n' && r != '\t') || unicode.Is(unicode.Cf, r) {
			t.Fatalf("unsafe rune %U", r)
		}
	}
	if err := f.Write("transcript", []byte(`{"source":"pi","type":"pi.output","data":{"hello":"raw"}}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"source":"pi"`) {
		t.Fatal("missing unsupported raw")
	}
	transcriptChunk(t, f, "b", "stdout", 0, `{"unfinished":`, false, 0)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "transcript reader stopped") {
		t.Fatal(out.String())
	}
}

type transcriptShortWriter struct{}

func TestTranscriptExactLineBoundAndEmptyEOF(t *testing.T) {
	for _, extra := range []int{0, 1} {
		var out bytes.Buffer
		f := NewTranscriptFormatter(&out)
		prefix := `{"type":"item.completed","item":{"id":"x","type":"agent_message","text":"`
		suffix := `"}}`
		line := prefix + strings.Repeat("z", maxLineBytes-len(prefix)-len(suffix)+extra) + suffix
		for offset := 0; offset < len(line); offset += pageMax {
			transcriptChunk(t, f, "a", "stdout", uint64(offset), line[offset:min(len(line), offset+pageMax)], false, 0)
		}
		transcriptChunk(t, f, "a", "stdout", uint64(len(line)), "", true, 0)
		if strings.Contains(out.String(), "[Codex agent-reported message]") != (extra == 0) {
			t.Fatalf("line length %d: wrong interpretation", len(line))
		}
		if extra == 1 && !strings.Contains(out.String(), "oversized line") {
			t.Fatal("missing oversize fallback")
		}
	}
}

func TestTranscriptMalformedEnvelopesAndInitialRetention(t *testing.T) {
	valid := map[string]any{"executionId": "a", "stream": "stdout", "offset": 0, "nextOffset": 1, "retainedFrom": 0, "producedEnd": 1, "eof": true, "data": "eA=="}
	for key, value := range map[string]any{"offset": -1, "nextOffset": 2, "retainedFrom": 1, "producedEnd": 0, "eof": nil, "data": "not base64", "stream": "stdin"} {
		t.Run(key, func(t *testing.T) {
			chunk := make(map[string]any)
			for k, v := range valid {
				chunk[k] = v
			}
			chunk[key] = value
			body, _ := json.Marshal(map[string]any{"source": "codex", "type": "codex.process-output", "data": chunk})
			var out bytes.Buffer
			if err := NewTranscriptFormatter(&out).Write("transcript", body); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "invalid process envelope") {
				t.Fatal(out.String())
			}
		})
	}
	var out bytes.Buffer
	f := NewTranscriptFormatter(&out)
	transcriptChunk(t, f, "a", "stdout", 9, "tail\n"+messageLine+"\n", true, 9)
	if !strings.Contains(out.String(), "process gap") || !strings.Contains(out.String(), "hello 界🙂") {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := f.Write("transcript", bytes.Repeat([]byte("a"), maxEnvelopeBytes+1)); err != nil {
		t.Fatal(err)
	}
	if out.Len() > maxFallbackBytes+256 || !strings.Contains(out.String(), "output truncated") {
		t.Fatal("unbounded envelope fallback")
	}
}

func (transcriptShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestTranscriptWriterFailure(t *testing.T) {
	f := NewTranscriptFormatter(transcriptShortWriter{})
	if err := f.Write("client-gap", []byte(`{}`)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err=%v", err)
	}
}
