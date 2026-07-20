// Package processoutput provides the bounded managed-process output transport
// shared by foreground adapters. Agent argv and terminal records remain owned
// by each adapter.
package processoutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Chris-Cullins/swe-platform/internal/controllers"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const outputPageMax = 64 * 1024

// Forwarder emits bounded, cursor-based stdout and stderr events. Its zero
// value is ready for use.
type Forwarder struct {
	mu      sync.Mutex
	cursors map[outputCursor]uint64
}

type outputCursor struct {
	environment string
	owner       string
	execution   string
	stream      sandboxdv1.OutputStream
}

type outputEvent struct {
	ExecutionID  string `json:"executionId"`
	Stream       string `json:"stream"`
	Offset       uint64 `json:"offset"`
	NextOffset   uint64 `json:"nextOffset"`
	GapBytes     uint64 `json:"gapBytes,omitempty"`
	RetainedFrom uint64 `json:"retainedFrom"`
	ProducedEnd  uint64 `json:"producedEnd"`
	EOF          bool   `json:"eof"`
	Data         []byte `json:"data,omitempty"`
}

// Forward reads all currently available retained output for process and emits
// adapter-owned events. Cursors advance only after a successful append, while
// content-addressed keys make replay after a restart idempotent.
func (f *Forwarder) Forward(
	ctx context.Context,
	client sandboxdv1.ProcessServiceClient,
	key *sandboxdv1.ProcessKey,
	sandbox controllers.AdapterSandbox,
	process *sandboxdv1.Process,
	source string,
	eventType string,
) error {
	if sandbox.EmitEvent == nil || process.ExecutionId == "" {
		return nil
	}
	for _, stream := range []sandboxdv1.OutputStream{sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR} {
		cursor := outputCursor{environment: string(sandbox.EnvironmentUID), owner: key.OwnerId, execution: process.ExecutionId, stream: stream}
		offset := f.cursor(cursor)
		for {
			response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{
				Key: key, ExecutionId: process.ExecutionId, Stream: stream, Offset: offset, MaxBytes: outputPageMax,
			})
			if err != nil {
				return err
			}
			if len(response.Data) == 0 && response.GapBytes == 0 {
				break
			}
			payload, err := json.Marshal(outputEvent{
				ExecutionID: process.ExecutionId, Stream: streamName(stream), Offset: response.Offset,
				NextOffset: response.NextOffset, GapBytes: response.GapBytes, RetainedFrom: response.RetainedStart,
				ProducedEnd: response.ProducedEnd, EOF: response.Eof, Data: response.Data,
			})
			if err != nil {
				return err
			}
			digest := sha256.Sum256(payload)
			idempotencyKey := fmt.Sprintf("v1:%s:%x", streamName(stream), digest)
			if err := sandbox.EmitEvent(ctx, controllers.AdapterEvent{
				Source: source, IdempotencyKey: idempotencyKey, Type: eventType, Data: payload,
			}); err != nil {
				return err
			}
			offset = response.NextOffset
			f.setCursor(cursor, offset)
			if response.Eof || offset >= response.ProducedEnd {
				break
			}
		}
	}
	return nil
}

// ReadRetained returns the currently retained bytes for one process stream.
func ReadRetained(ctx context.Context, client sandboxdv1.ProcessServiceClient, key *sandboxdv1.ProcessKey, executionID string, stream sandboxdv1.OutputStream) ([]byte, error) {
	var output bytes.Buffer
	var offset uint64
	for {
		response, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{
			Key: key, ExecutionId: executionID, Stream: stream, Offset: offset, MaxBytes: outputPageMax,
		})
		if err != nil {
			return nil, err
		}
		output.Write(response.Data)
		offset = response.NextOffset
		if response.Eof || offset >= response.ProducedEnd {
			return output.Bytes(), nil
		}
	}
}

func streamName(stream sandboxdv1.OutputStream) string {
	if stream == sandboxdv1.OutputStream_OUTPUT_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
}

func (f *Forwarder) cursor(key outputCursor) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cursors == nil {
		f.cursors = make(map[outputCursor]uint64)
	}
	return f.cursors[key]
}

func (f *Forwarder) setCursor(key outputCursor, offset uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cursors == nil {
		f.cursors = make(map[outputCursor]uint64)
	}
	f.cursors[key] = offset
}
