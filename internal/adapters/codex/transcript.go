package codex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxLineBytes     = 256 << 10
	maxEnvelopeBytes = 128 << 10
	maxOutputBytes   = 16 << 10
	maxFallbackBytes = 4 << 10
	maxExecutions    = 64
)

// TranscriptFormatter is an opt-in presentation of the pinned rust-v0.144.6
// exec JSONL contract, not a transcript schema or per-event version assertion.
// See https://github.com/openai/codex/blob/rust-v0.144.6/codex-rs/exec/src/exec_events.rs.
// It retains at most two 256 KiB partial lines and two 256 KiB replay windows.
type TranscriptFormatter struct {
	out       io.Writer
	execution string
	seen      map[string]bool
	streams   [2]transcriptStream
	disabled  bool
}

type transcriptStream struct {
	started bool
	next    uint64
	tail    []byte
	line    []byte
	discard bool
}

func NewTranscriptFormatter(out io.Writer) *TranscriptFormatter {
	return &TranscriptFormatter{out: out, seen: make(map[string]bool)}
}

// Write accepts transport event names and opaque transcript envelopes. Unknown
// adapters/events are explicitly shown as bounded raw text, never interpreted.
func (f *TranscriptFormatter) Write(kind string, data []byte) error {
	if kind != "transcript" {
		if err := f.boundary("transport boundary"); err != nil {
			return err
		}
		return f.raw("transport "+kind, data)
	}
	var event struct {
		Source string          `json:"source"`
		Type   string          `json:"type"`
		Data   json.RawMessage `json:"data"`
	}
	if len(data) > maxEnvelopeBytes || json.Unmarshal(data, &event) != nil || event.Source != "codex" || event.Type != "codex.process-output" {
		if err := f.boundary("unsupported or malformed transcript event"); err != nil {
			return err
		}
		return f.raw("unsupported or malformed transcript event", data)
	}
	var chunk outputEvent
	var required map[string]json.RawMessage
	if json.Unmarshal(event.Data, &chunk) != nil || json.Unmarshal(event.Data, &required) != nil ||
		chunk.ExecutionID == "" || len(chunk.ExecutionID) > 256 || (chunk.Stream != "stdout" && chunk.Stream != "stderr") ||
		len(chunk.Data) > pageMax || chunk.NextOffset < chunk.Offset || chunk.NextOffset-chunk.Offset != uint64(len(chunk.Data)) ||
		chunk.RetainedFrom > chunk.Offset || chunk.GapBytes > chunk.Offset || chunk.ProducedEnd < chunk.NextOffset || (chunk.EOF && chunk.ProducedEnd != chunk.NextOffset) ||
		missing(required, "offset", "nextOffset", "retainedFrom", "producedEnd", "eof") {
		if err := f.boundary("invalid process envelope"); err != nil {
			return err
		}
		return f.raw("invalid process envelope", event.Data)
	}
	if f.disabled {
		return f.raw("execution tracking limit reached", event.Data)
	}
	if chunk.ExecutionID != f.execution {
		if f.seen[chunk.ExecutionID] {
			return f.raw("retired execution replay; not reinterpreted", event.Data)
		}
		if err := f.boundary("execution changed"); err != nil {
			return err
		}
		f.streams = [2]transcriptStream{}
		if len(f.seen) == maxExecutions {
			f.disabled = true
			return f.raw("execution tracking limit reached", event.Data)
		}
		f.execution = chunk.ExecutionID
		f.seen[f.execution] = true
		if err := f.emit("Codex execution (agent output)", []byte(f.execution), maxOutputBytes); err != nil {
			return err
		}
	}
	index := 0
	if chunk.Stream == "stderr" {
		index = 1
	}
	s := &f.streams[index]
	data = chunk.Data
	if s.started && chunk.Offset < s.next {
		overlap := min(s.next-chunk.Offset, uint64(len(data)))
		start := s.next - uint64(len(s.tail))
		if chunk.Offset < start || !bytes.Equal(data[:overlap], s.tail[chunk.Offset-start:chunk.Offset-start+overlap]) {
			if err := f.flush(s, chunk.Stream+" conflicting or unverifiable overlap"); err != nil {
				return err
			}
			s.discard = true
			return f.raw("conflicting or unverifiable overlap; chunk not interpreted", data)
		}
		data = data[overlap:]
		if len(data) == 0 {
			if chunk.EOF && chunk.NextOffset == s.next && len(s.line) > 0 {
				if err := f.line(chunk.Stream, s.line); err != nil {
					return err
				}
				s.line = nil
			}
			return nil
		}
	}
	if (!s.started && chunk.Offset != 0) || (s.started && chunk.Offset > s.next) || (chunk.GapBytes > 0 && chunk.Offset >= s.next) {
		if err := f.flush(s, chunk.Stream+" incomplete before gap"); err != nil {
			return err
		}
		if err := f.emit("Codex process gap", []byte(fmt.Sprintf("%s expected=%d offset=%d gapBytes=%d; resuming at next newline", chunk.Stream, s.next, chunk.Offset, chunk.GapBytes)), maxOutputBytes); err != nil {
			return err
		}
		s.tail = nil
		s.discard = true
	}
	s.started = true
	s.next = chunk.NextOffset
	s.tail = append(s.tail, data...)
	if len(s.tail) > maxLineBytes {
		s.tail = bytes.Clone(s.tail[len(s.tail)-maxLineBytes:])
	}
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		end := n
		if end < 0 {
			end = len(data)
		}
		part := data[:end]
		if s.discard {
			if err := f.raw(chunk.Stream+" fragment after loss or oversized line", part); err != nil {
				return err
			}
		} else if len(s.line)+len(part) > maxLineBytes {
			if err := f.raw(chunk.Stream+" oversized line prefix", append(s.line, part[:min(len(part), maxFallbackBytes)]...)); err != nil {
				return err
			}
			s.line = nil
			s.discard = true
		} else {
			s.line = append(s.line, part...)
		}
		if n < 0 {
			break
		}
		if !s.discard {
			if err := f.line(chunk.Stream, s.line); err != nil {
				return err
			}
		}
		s.line = nil
		s.discard = false
		data = data[n+1:]
	}
	if chunk.EOF && len(s.line) > 0 {
		if err := f.line(chunk.Stream, s.line); err != nil {
			return err
		}
		s.line = nil
	}
	return nil
}

func missing(fields map[string]json.RawMessage, names ...string) bool {
	for _, name := range names {
		if len(fields[name]) == 0 || string(fields[name]) == "null" {
			return true
		}
	}
	return false
}

func (f *TranscriptFormatter) boundary(reason string) error {
	for i := range f.streams {
		if err := f.flush(&f.streams[i], reason+"; incomplete line"); err != nil {
			return err
		}
		f.streams[i].discard = true
	}
	return nil
}

func (f *TranscriptFormatter) flush(s *transcriptStream, reason string) error {
	line := s.line
	s.line = nil
	if len(line) != 0 {
		return f.raw(reason, line)
	}
	return nil
}

// Close exposes a partial line if the caller stops before process EOF. It does
// not claim that the process or platform task completed.
func (f *TranscriptFormatter) Close() error { return f.boundary("transcript reader stopped") }

func (f *TranscriptFormatter) line(stream string, line []byte) error {
	if stream == "stderr" {
		return f.emit("Codex stderr (agent output)", line, maxOutputBytes)
	}
	var record struct {
		Type     string                     `json:"type"`
		ThreadID *string                    `json:"thread_id"`
		Usage    map[string]json.RawMessage `json:"usage"`
		Error    *struct {
			Message *string `json:"message"`
		} `json:"error"`
		Item *struct {
			ID       *string         `json:"id"`
			Type     string          `json:"type"`
			Text     *string         `json:"text"`
			Command  *string         `json:"command"`
			Output   *string         `json:"aggregated_output"`
			ExitCode json.RawMessage `json:"exit_code"`
			Status   string          `json:"status"`
		} `json:"item"`
	}
	if !utf8.Valid(line) || json.Unmarshal(line, &record) != nil {
		return f.raw("malformed Codex JSONL", line)
	}
	switch record.Type {
	case "item.completed":
		item := record.Item
		if item == nil || item.ID == nil {
			break
		}
		if item.Type == "agent_message" && item.Text != nil {
			return f.emit("Codex agent-reported message", []byte(*item.Text), maxOutputBytes)
		}
		if item.Type == "command_execution" && item.Command != nil && item.Output != nil && len(item.ExitCode) != 0 {
			var code *int64
			if json.Unmarshal(item.ExitCode, &code) != nil {
				break
			}
			if item.Status != "completed" && item.Status != "failed" && item.Status != "declined" && item.Status != "in_progress" {
				break
			}
			return f.emit("Codex agent-reported command (not platform verification)", []byte(fmt.Sprintf("command: %s\nstatus: %s; exit_code: %s\n%s", *item.Command, item.Status, item.ExitCode, *item.Output)), maxOutputBytes)
		}
	case "thread.started":
		if record.ThreadID == nil {
			break
		}
		return f.emit("Codex agent-reported metadata", line, maxOutputBytes)
	case "turn.started":
		return f.emit("Codex agent-reported metadata", line, maxOutputBytes)
	case "turn.completed":
		if record.Usage == nil {
			break
		}
		for _, name := range []string{"input_tokens", "cached_input_tokens", "cache_write_input_tokens", "output_tokens", "reasoning_output_tokens"} {
			var count int64
			if missing(record.Usage, name) || json.Unmarshal(record.Usage[name], &count) != nil {
				return f.raw("malformed Codex usage metadata", line)
			}
		}
		return f.emit("Codex agent-reported metadata (usage is not accounting)", line, maxOutputBytes)
	case "turn.failed":
		if record.Error == nil || record.Error.Message == nil {
			break
		}
		return f.emit("Codex agent-reported metadata", line, maxOutputBytes)
	}
	return f.raw("unknown or malformed Codex record", line)
}

func (f *TranscriptFormatter) raw(reason string, data []byte) error {
	return f.emit("raw fallback: "+reason, data, maxFallbackBytes)
}

func (f *TranscriptFormatter) emit(label string, data []byte, limit int) error {
	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(sanitized([]byte(label), maxFallbackBytes))
	b.WriteString("]\n")
	b.WriteString(sanitized(data, limit))
	b.WriteByte('\n')
	text := b.String()
	n, err := io.WriteString(f.out, text)
	if err == nil && n != len(text) {
		err = io.ErrShortWrite
	}
	return err
}

func sanitized(data []byte, limit int) string {
	var b strings.Builder
	for len(data) > 0 {
		r, n := utf8.DecodeRune(data)
		text := string(r)
		if (unicode.IsControl(r) && r != '\n' && r != '\t') || unicode.Is(unicode.Cf, r) {
			text = fmt.Sprintf("\\u%04x", r)
		}
		if b.Len()+len(text) > limit {
			b.WriteString("\n[output truncated; default NDJSON preserves retained raw bytes]")
			break
		}
		b.WriteString(text)
		data = data[n:]
	}
	return b.String()
}
