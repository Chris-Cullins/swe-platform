# sandboxd process lifecycle contract (v1alpha1)

This document is versioned with `sandboxd.proto` and is normative. The API is the
same logical contract on Linux and Windows and exposes no PID, signal, process-group,
job, container, or other OS handle.

## Identity and states

`ProcessService` is the retry-safe API. A normalized `(owner_id, role, ProcessSpec)`
key identifies exactly one launch during a sandboxd daemon epoch. Concurrent starts
and a retry after an unknown RPC result return the same opaque `execution_id`; a
different spec fails. IDs are meaningful only in that epoch. Records and output are
bounded; once the record limit is reached new keys fail rather than evicting a key and
making a retry unsafe. Epoch close fences every execution tree before replacement.

Committed states include natural `RUNNING -> EXITED` and controlled
`RUNNING -> STOPPING -> EXITED`, with `FAILED` terminal for a
start or non-exit wait failure. Start failure is directly `FAILED`. `EXITED` is not
published until the leader is reaped and both output pipes have drained. Stop,
timeout, daemon close, and natural exit race by first accepted terminal cause; the
corresponding `TerminationReason` never changes. Exit code is present only after a
started child is reaped.

## Input, disconnect, timeout, and control

`Exec` is connection-bound convenience and **must not be retried after an uncertain
result**. Its first message is start. `stdin_eof`, or the gRPC `CloseSend` half-close,
closes child stdin while stdout/stderr and the exit remain readable. Full cancellation
or transport failure force-kills the execution tree. Timeout is measured from a
successful start. `INTERRUPT` requests the platform's portable interrupt behavior;
`TERMINATE` requests termination; `FORCE` is unconditional. A graceful managed stop
requests an interrupt where supported, retains `STOPPING` and continues draining output
even if the direct leader exits, then force-kills remaining descendants at the grace
deadline. An explicit `FORCE` may escalate earlier. Unsupported non-force controls
must fail explicitly; they must never be reported as successfully delivered.

Timeout, explicit control, disconnect, and daemon close (`DAEMON_CLOSED`) all contain the complete
descendant tree. Implementations fence descendants and drain output before terminal
state. Unix uses a private process group. Windows assigns every successful launch to a
private Job Object configured with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. The Job-list
process-thread attribute atomically creates the leader in that Job, with no runnable
pre-assignment window. Terminate, force, and daemon close terminate the Job and retain
its handle until Job accounting reports zero active processes. Reliable console
interrupt delivery is unavailable there, so explicit `INTERRUPT` is `Unimplemented`;
graceful stop closes (or already has EOF on) stdin, waits the grace period, then
terminates the Job Object. `TERMINATE` remains supported as immediate Job termination.

## Output

Managed stdout and stderr are permanently and independently drained into fixed-size
newest-byte buffers, so a child cannot block on a disconnected or slow reader.
`ReadOutput` uses absolute byte offsets and requires the execution ID. It returns the
actual first offset, exact `gap_bytes` when older bytes were discarded, `next_offset`,
`retained_start`, `produced_end`, and `eof` only when that stream is drained at its
current end. Reads and response sizes are bounded. Exec output chunks likewise carry
offsets and observable loss; delivery from pipe drains is bounded and non-blocking,
and a discarded tail is reported by a final zero-data gap chunk before exit.

## Environment and security

Empty/`INHERIT` mode means daemon environment plus deterministic key-sorted overrides;
`REPLACE` means only the sorted supplied map. CWD defaults to workspace. Environment
values are opaque; sandboxd neither detects credentials nor injects any automatically.
Credential provisioning is an external environment-layer concern.
No shell is implied. Invalid environment names and unknown modes/controls are rejected.

## Adapter shapes

* One possible foreground consumer starts one Run-keyed agent process, polls `Get`, reads each
  output cursor, and maps terminal reason/exit to adapter-owned Run semantics.
* One possible long-lived-service consumer starts one Environment-keyed service, reconnects with
  `Get` and `ReadOutput`, and submits retry-safe task IDs through that service's own
  protocol. Service process exit is not assumed to equal task completion.
