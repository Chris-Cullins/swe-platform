# sandboxd workspace filesystem contract (v1alpha1)

This document is versioned with `sandboxd.proto` and is normative. `FilesystemService`
is a bounded workspace capability, not arbitrary container filesystem access. A future
whole-container capability, if needed, must be a separately authorized service.

## Logical paths and confinement

Paths are workspace-relative UTF-8 logical paths with `/` separators on every host.
The empty path denotes the workspace root. Empty components and `.` are normalized
away. A leading `/`, `..` component, `\`, NUL, Windows drive prefix (such as `C:`), or
UNC/device form is invalid regardless of the server operating system. Responses use
logical names and never expose host paths or separators. Components beginning with
`.sandboxd-write-` are reserved for hidden staging files and are rejected. For the same
syntax on every host, control characters, Windows-invalid `<>:"|?*`, trailing dots or
spaces, and reserved device names such as `NUL`, `CONIN$`, `COM1`, and their aliases are
also invalid.

The workspace root is opened and fixed before the gRPC server starts. Resolution uses
that root's race-safe host handles: a relative symlink may be followed only when its
target remains inside the fixed workspace. Dangling, absolute, and outside-workspace
links fail; writes reject an encountered symlink and always refuse to replace a symlink
at the final destination. `List`
reports a direct child link as `ENTRY_TYPE_SYMLINK` without following it. These rules
prevent escapes even if another workspace process swaps a checked component; callers
with `Exec` remain able to access whatever that separate capability permits.

## Reads and metadata

`Read` returns at most the server maximum (256 KiB by default); zero `max_bytes` uses
64 KiB. Clients continue from `next_offset` until `eof`. `offset` beyond the current
size is `OutOfRange`. `size` describes the complete regular file. When
`include_version` is true, `version` is its lowercase SHA-256 digest; clients normally
request this once and omit it while paging the remaining content. Hashing and transfer
use fixed-size buffers, so files larger than gRPC's default message limit do not require
whole-file memory.

Metadata is portable: entry type, regular-file byte size (zero for other types), and
Unix-millisecond modification time.
No POSIX owner, group, or mode is required or fabricated on Windows. Filesystem objects
other than regular files and directories can be listed as `ENTRY_TYPE_OTHER` but cannot
be read or written through this capability.

## Writes, optimistic replacement, and cancellation

`Write` is a client stream. Exactly one header comes first, followed by bounded data
messages (256 KiB each by default). A stream is limited to 1 GiB by default, and the
server admits at most 16 concurrent streams by default. Cancellation observed before
publication and malformed, over-limit, or interrupted streams remove their staging file
and leave the destination unchanged.

Data is staged in the destination directory, flushed, closed, and published with the
host's same-filesystem atomic replacement operation only after the complete stream is
received. Staging names cannot be addressed or listed through this capability. `ANY`
replaces any regular file; `MUST_NOT_EXIST` uses atomic no-replace publication and
creates only when absent. `MATCH_VERSION` replaces only when the current complete-file
SHA-256 equals `expected_version`. Hash checking and replacement are serialized with
other writes in this server, so concurrent RPC writers cannot both commit.
Non-cooperating processes with `Exec` can modify workspace files outside this
serialization, so `MATCH_VERSION` is not a global filesystem compare-and-swap.
Cancellation observed before publication leaves the destination unchanged; as with any
committing RPC, cancellation racing with publication can produce an unknown successful
result. A successful response returns the committed size/version.
The API intentionally has no Unix mode field; newly created files use server defaults.

## Listings, limits, errors, and mutation

`List` returns at most 1,000 direct entries (256 by default). Its opaque page token is
scoped to that logical directory and server implementation. Responses hold only one
page in memory. If the directory changes between pages, the listing is weakly
consistent and clients may restart from an empty token to obtain a fresh traversal.

Malformed paths, tokens, headers, and limits are `InvalidArgument`; stale offsets are
`OutOfRange`; missing objects are `NotFound`; symlinks, wrong object types, and failed
write preconditions are `FailedPrecondition`; stream/file/concurrency bounds are
`ResourceExhausted`; cancellation is `Canceled` or `DeadlineExceeded`; and unexpected
host I/O failures are `Internal`. RPC contexts are checked during hashing, transfer,
directory scans, staging, and commit.

Separate ranged reads are weakly consistent when another process changes the file
between calls. Clients that need a stable edit base request a version and use
`MATCH_VERSION` when publishing their replacement.
