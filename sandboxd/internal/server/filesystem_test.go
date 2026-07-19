package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func newFilesystemConn(t *testing.T, fs *FilesystemServer) *grpc.ClientConn {
	t.Helper()
	if err := fs.initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterFilesystemServiceServer(grpcServer, fs)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough://filesystem",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTestFilesystemServer(t *testing.T, workspace string) *FilesystemServer {
	t.Helper()
	server, err := NewFilesystemServer(workspace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func writeWorkspaceFile(ctx context.Context, client sandboxdv1.FilesystemServiceClient, header *sandboxdv1.WriteHeader, content []byte) (*sandboxdv1.WriteResponse, error) {
	stream, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: header}}); err != nil {
		return nil, err
	}
	for len(content) > 0 {
		n := min(len(content), 64*1024)
		if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Data{Data: content[:n]}}); err != nil {
			return nil, err
		}
		content = content[n:]
	}
	return stream.CloseAndRecv()
}

func TestFilesystemLargeFileRoundTripIsChunked(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	content := bytes.Repeat([]byte("large-workspace-file\n"), 300000) // exceeds default gRPC message limits
	written, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{
		Path: "notes/large.txt", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MUST_NOT_EXIST,
	}, content)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	wantHash := sha256.Sum256(content)
	if written.Size != uint64(len(content)) || written.Version != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("write metadata = %+v", written)
	}

	var got []byte
	var offset uint64
	for {
		page, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "notes/large.txt", Offset: offset, MaxBytes: 128 * 1024, IncludeVersion: offset == 0})
		if err != nil {
			t.Fatalf("read at %d: %v", offset, err)
		}
		if len(page.Data) > 128*1024 || page.Offset != offset || page.Size != uint64(len(content)) || offset == 0 && page.Version != written.Version || offset > 0 && page.Version != "" {
			t.Fatalf("invalid page at %d: %+v", offset, page)
		}
		got = append(got, page.Data...)
		offset = page.NextOffset
		if page.Eof {
			break
		}
	}
	if !bytes.Equal(got, content) {
		t.Fatal("chunked content mismatch")
	}
	if _, err := os.Stat(filepath.Join(workspace, "notes", "large.txt")); err != nil {
		t.Fatalf("file not in workspace: %v", err)
	}
}

func TestFilesystemRejectsNonPortableAndEscapingPaths(t *testing.T) {
	server := newTestFilesystemServer(t, t.TempDir())
	for _, value := range []string{"/etc/passwd", "../outside", "a/../../outside", `C:\\Windows`, "C:/Windows", `\\\\server\\share`, "a\\b", "a:b", "x\x00y", "a?b", "a<b", "control\x01", "NUL", "com1.txt", "COM¹", "lpt².log", "CONIN$", "conout$.txt", "trailing.", "trailing ", ".SANDBOXD-WRITE-secret"} {
		t.Run(strings.ReplaceAll(value, "/", "_"), func(t *testing.T) {
			_, err := server.Read(context.Background(), &sandboxdv1.ReadRequest{Path: value})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("path %q returned %v", value, err)
			}
		})
	}
	for input, want := range map[string]string{"a//b": "a/b", "./a/./b": "a/b", "": ""} {
		got, err := normalizeWorkspacePath(input, true)
		if err != nil || got != want {
			t.Fatalf("normalize %q = %q, %v; want %q", input, got, err, want)
		}
	}
}

func TestFilesystemRejectsNestedAndDanglingSymlinksAndListsTheirType(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "inside"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "inside", "ok"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "nested")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink privilege unavailable: %v", err)
		}
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "missing"), filepath.Join(workspace, "dangling")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("inside", filepath.Join(workspace, "internal")); err != nil {
		t.Fatal(err)
	}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	for _, path := range []string{"nested/secret", "dangling"} {
		if _, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: path}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("read %s: %v", path, err)
		}
		_, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{Path: path, Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}, []byte("overwrite"))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	read, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "internal/ok"})
	if err != nil || string(read.Data) != "inside" {
		t.Fatalf("confined internal link: %q, %v", read.GetData(), err)
	}
	if _, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{Path: "internal/new", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}, []byte("written")); err != nil {
		t.Fatalf("write through confined internal link: %v", err)
	}
	assertFileContent(t, filepath.Join(workspace, "inside", "new"), "written")
	list, err := client.List(context.Background(), &sandboxdv1.ListRequest{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range list.Entries {
		if entry.Name == "nested" || entry.Name == "dangling" {
			if entry.Type != sandboxdv1.EntryType_ENTRY_TYPE_SYMLINK {
				t.Fatalf("symlink entry = %+v", entry)
			}
		}
	}
}

func TestFilesystemListPaginationAndPortableTypes(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "file"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".SANDBOXD-WRITE-hidden"), []byte("staging"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	var entries []*sandboxdv1.Entry
	var token string
	for {
		page, err := client.List(context.Background(), &sandboxdv1.ListRequest{PageSize: 1, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) > 1 {
			t.Fatalf("unbounded page: %d", len(page.Entries))
		}
		entries = append(entries, page.Entries...)
		token = page.NextPageToken
		if token == "" {
			break
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if len(entries) != 2 || entries[0].Name != "directory" || entries[0].Type != sandboxdv1.EntryType_ENTRY_TYPE_DIRECTORY || entries[1].Name != "file" || entries[1].Type != sandboxdv1.EntryType_ENTRY_TYPE_FILE || entries[1].Size != 3 {
		t.Fatalf("entries = %+v", entries)
	}
	_, err := client.List(context.Background(), &sandboxdv1.ListRequest{Path: "directory", PageToken: encodePageToken("", 1)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-directory token: %v", err)
	}
}

func TestFilesystemInterruptedAndLimitedWritesPreserveDestination(t *testing.T) {
	workspace := t.TempDir()
	destination := filepath.Join(workspace, "target")
	if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FilesystemServer{Workspace: workspace, MaxWriteChunkBytes: 4, MaxWriteBytes: 6}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, server))

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "target", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Data{Data: []byte("part")}}); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled write: %v", err)
	}
	waitFor(t, func() bool {
		matches, _ := filepath.Glob(filepath.Join(workspace, ".sandboxd-write-*"))
		return len(matches) == 0
	})
	assertFileContent(t, destination, "original")

	for name, content := range map[string][]byte{"chunk": []byte("12345"), "file": []byte("1234") /* followed below */} {
		stream, err := client.Write(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "target", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Data{Data: content}}); err != nil {
			t.Fatal(err)
		}
		if name == "file" {
			if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Data{Data: []byte("789")}}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = stream.CloseAndRecv()
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("%s limit: %v", name, err)
		}
		assertFileContent(t, destination, "original")
	}
}

func TestFilesystemCanceledWriteCleansPinnedRenamedDirectory(t *testing.T) {
	workspace := t.TempDir()
	parent := filepath.Join(workspace, "parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	server := newTestFilesystemServer(t, workspace)
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, server))
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "parent/file", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		matches, _ := filepath.Glob(filepath.Join(parent, ".sandboxd-write-*"))
		return len(matches) == 1
	})
	moved := filepath.Join(workspace, "moved")
	if err := os.Rename(parent, moved); err != nil {
		if runtime.GOOS != "windows" || !errors.Is(err, os.ErrPermission) {
			t.Fatal(err)
		}
		// Windows denies the rename while the pinned directory handle is
		// open, preventing the swap rather than tracking it across a rename.
		cancel()
		if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Canceled {
			t.Fatalf("cancelled write: %v", err)
		}
		waitFor(t, func() bool {
			matches, _ := filepath.Glob(filepath.Join(parent, ".sandboxd-write-*"))
			return len(matches) == 0
		})
		return
	}
	cancel()
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled write: %v", err)
	}
	waitFor(t, func() bool {
		matches, _ := filepath.Glob(filepath.Join(moved, ".sandboxd-write-*"))
		return len(matches) == 0
	})
	if _, err := os.Stat(filepath.Join(moved, "file")); !os.IsNotExist(err) {
		t.Fatalf("canceled destination exists: %v", err)
	}
}

func TestFilesystemErrorsDoNotExposeHostPaths(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	_, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "missing/file"})
	if status.Code(err) != codes.NotFound || strings.Contains(err.Error(), workspace) || strings.Contains(err.Error(), `\`) || strings.Contains(err.Error(), "missing/file") {
		t.Fatalf("host path leaked in error: %v", err)
	}
	hostErr := &os.PathError{Op: "open", Path: `C:\\private\\workspace\\file`, Err: os.ErrPermission}
	sanitized := filesystemError("open workspace file", hostErr)
	if status.Code(sanitized) != codes.PermissionDenied || strings.Contains(sanitized.Error(), "private") || strings.Contains(sanitized.Error(), `\`) {
		t.Fatalf("synthetic host path leaked in error: %v", sanitized)
	}
	uninitialized := &FilesystemServer{Workspace: filepath.Join(workspace, "absent")}
	_, err = uninitialized.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "file"})
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), workspace) {
		t.Fatalf("initialization host path leaked in error: %v", err)
	}
}

func TestFilesystemOptimisticConcurrentWritesCommitOnce(t *testing.T) {
	workspace := t.TempDir()
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	base, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{Path: "race", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MUST_NOT_EXIST}, []byte("base"))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, value := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{Path: "race", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MATCH_VERSION, ExpectedVersion: base.Version}, []byte(value))
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	var succeeded, conflicted int
	for err := range errs {
		switch status.Code(err) {
		case codes.OK:
			succeeded++
		case codes.FailedPrecondition:
			conflicted++
		default:
			t.Fatalf("write error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}
	content, err := os.ReadFile(filepath.Join(workspace, "race"))
	if err != nil || string(content) != "first" && string(content) != "second" {
		t.Fatalf("committed content = %q, %v", content, err)
	}
}

func TestFilesystemSymlinkSwapCannotEscapeRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory/symlink swap semantics are covered by os.Root on Windows")
	}
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "swap"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "swap", "value"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "value"), []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		parked := filepath.Join(workspace, "parked")
		for {
			select {
			case <-stop:
				return
			default:
			}
			if os.Rename(filepath.Join(workspace, "swap"), parked) != nil {
				continue
			}
			_ = os.Symlink(outside, filepath.Join(workspace, "swap"))
			runtime.Gosched()
			_ = os.Remove(filepath.Join(workspace, "swap"))
			_ = os.Rename(parked, filepath.Join(workspace, "swap"))
		}
	}()
	for range 500 {
		read, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "swap/value", MaxBytes: 64})
		if err == nil && string(read.Data) != "inside" {
			close(stop)
			<-done
			t.Fatalf("workspace escape returned %q", read.Data)
		}
	}
	close(stop)
	<-done
}

func TestReadChunkRejectsShortReadWithoutExposingBufferTail(t *testing.T) {
	for name, input := range map[string]string{"immediate EOF": "", "partial read": "x"} {
		t.Run(name, func(t *testing.T) {
			data, err := readChunk(strings.NewReader(input), 4)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("short read error = %v; want %v", err, io.ErrUnexpectedEOF)
			}
			if data != nil {
				t.Fatalf("short read returned %d bytes from incomplete buffer", len(data))
			}
		})
	}
}

func TestFilesystemConcurrentMutationPreservesReadResponseInvariants(t *testing.T) {
	workspace := t.TempDir()
	path := filepath.Join(workspace, "changing")
	content := bytes.Repeat([]byte("x"), 256*1024)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, newTestFilesystemServer(t, workspace)))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Truncate(path, 1)
			_ = os.WriteFile(path, content, 0o600)
		}
	}()
	for range 200 {
		read, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "changing", MaxBytes: 128 * 1024})
		if err != nil && status.Code(err) != codes.Aborted {
			close(stop)
			<-done
			t.Fatalf("read during truncate: %v", err)
		}
		if err == nil && (read.Offset != 0 || read.NextOffset != uint64(len(read.Data)) || len(read.Data) > 128*1024 || read.NextOffset > read.Size || len(read.Data) == 0 && !read.Eof || read.Eof != (read.NextOffset == read.Size)) {
			close(stop)
			<-done
			t.Fatalf("incoherent read response: offset=%d next=%d size=%d eof=%v data=%d", read.Offset, read.NextOffset, read.Size, read.Eof, len(read.Data))
		}
	}
	close(stop)
	<-done
}

func TestFilesystemCancellationAndReadListLimits(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "file"), bytes.Repeat([]byte("x"), 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &FilesystemServer{Workspace: workspace, MaxReadBytes: 8, MaxPageSize: 1}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, server))
	if _, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "file", MaxBytes: 9}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("read limit: %v", err)
	}
	if _, err := client.Read(context.Background(), &sandboxdv1.ReadRequest{Path: "file", Offset: 1025}); status.Code(err) != codes.OutOfRange {
		t.Fatalf("read offset: %v", err)
	}
	if _, err := client.List(context.Background(), &sandboxdv1.ListRequest{PageSize: 2}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("list limit: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.Read(ctx, &sandboxdv1.ReadRequest{Path: "file", MaxBytes: 1}); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled read: %v", err)
	}
}

func TestFilesystemConcurrentWriteLimit(t *testing.T) {
	server := &FilesystemServer{Workspace: t.TempDir(), MaxConcurrentWrites: 1}
	if err := server.initialize(); err != nil {
		t.Fatal(err)
	}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, server))
	first, err := client.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "first", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(server.writeSlots) == 1 })
	second, err := client.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "second", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
		t.Fatal(err)
	}
	_, err = second.CloseAndRecv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("concurrent limit: %v", err)
	}
	if err := first.CloseSend(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestFilesystemShutdownFencesAndWaitsForWriteCleanup(t *testing.T) {
	workspace := t.TempDir()
	server := &FilesystemServer{Workspace: workspace}
	client := sandboxdv1.NewFilesystemServiceClient(newFilesystemConn(t, server))
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.WriteRequest{Kind: &sandboxdv1.WriteRequest_Header{Header: &sandboxdv1.WriteHeader{Path: "active", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(server.writeSlots) == 1 })
	server.BeginShutdown()
	if _, err := writeWorkspaceFile(context.Background(), client, &sandboxdv1.WriteHeader{Path: "rejected", Precondition: sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY}, nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("write admitted during shutdown: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	if err := server.CloseContext(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext with active write = %v", err)
	}
	cancel()
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled write: %v", err)
	}
	if err := server.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(workspace, ".sandboxd-write-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("staging files after shutdown = %v, %v", matches, err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf("%s = %q, %v; want %q", path, got, err, want)
	}
}
