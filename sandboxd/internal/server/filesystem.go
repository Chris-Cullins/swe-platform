package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	defaultReadBytes        = 64 * 1024
	defaultMaxReadBytes     = 256 * 1024
	defaultPageSize         = 256
	defaultMaxPageSize      = 1000
	defaultMaxWriteChunk    = 256 * 1024
	defaultMaxWriteBytes    = 1 << 30
	defaultConcurrentWrites = 16
	filesystemHashChunk     = 64 * 1024
)

// FilesystemServer implements the bounded workspace FilesystemService.
type FilesystemServer struct {
	sandboxdv1.UnimplementedFilesystemServiceServer
	Workspace           string
	MaxReadBytes        uint32
	MaxPageSize         uint32
	MaxWriteChunkBytes  uint32
	MaxWriteBytes       uint64
	MaxConcurrentWrites int

	initOnce   sync.Once
	root       string
	rootHandle *os.Root
	initErr    error
	writeSlots chan struct{}
	commitMu   sync.RWMutex
}

func NewFilesystemServer(workspace string) (*FilesystemServer, error) {
	server := &FilesystemServer{Workspace: workspace}
	if err := server.initialize(); err != nil {
		return nil, err
	}
	return server, nil
}

func (s *FilesystemServer) Close() error {
	if s.rootHandle == nil {
		return nil
	}
	return s.rootHandle.Close()
}

func (s *FilesystemServer) initialize() error {
	s.initOnce.Do(func() {
		root, err := filepath.Abs(s.Workspace)
		if err != nil {
			s.initErr = fmt.Errorf("workspace path: %w", err)
			return
		}
		s.rootHandle, err = os.OpenRoot(root)
		if err != nil {
			s.initErr = fmt.Errorf("open workspace root: %w", err)
			return
		}
		info, err := s.rootHandle.Stat(".")
		if err != nil || !info.IsDir() {
			if err == nil {
				err = errors.New("not a directory")
			}
			s.initErr = fmt.Errorf("workspace root: %w", err)
			_ = s.rootHandle.Close()
			s.rootHandle = nil
			return
		}
		s.root = filepath.Clean(root)
		if s.MaxReadBytes > defaultMaxReadBytes || s.MaxPageSize > defaultMaxPageSize || s.MaxWriteChunkBytes > defaultMaxWriteChunk || s.MaxWriteBytes > defaultMaxWriteBytes || s.MaxConcurrentWrites > defaultConcurrentWrites {
			s.initErr = errors.New("filesystem limit exceeds hard maximum")
			_ = s.rootHandle.Close()
			s.rootHandle = nil
			return
		}
		limit := s.MaxConcurrentWrites
		if limit <= 0 {
			limit = defaultConcurrentWrites
		}
		s.writeSlots = make(chan struct{}, limit)
	})
	return s.initErr
}

func normalizeWorkspacePath(value string, allowRoot bool) (string, error) {
	if strings.ContainsRune(value, 0) || strings.Contains(value, "\\") || strings.Contains(value, ":") || strings.HasPrefix(value, "/") {
		return "", status.Error(codes.InvalidArgument, "path must be a workspace-relative logical path")
	}
	for _, component := range strings.Split(value, "/") {
		if component == ".." {
			return "", status.Error(codes.InvalidArgument, "path must not contain upward traversal")
		}
		if strings.HasPrefix(component, ".sandboxd-write-") {
			return "", status.Error(codes.InvalidArgument, "path uses a reserved staging name")
		}
		if invalidWindowsComponent(component) {
			return "", status.Error(codes.InvalidArgument, "path contains a non-portable component")
		}
	}
	clean := pathpkg.Clean(value)
	if clean == "." {
		clean = ""
	}
	if clean == "" && !allowRoot {
		return "", status.Error(codes.InvalidArgument, "path must name a workspace entry")
	}
	return clean, nil
}

func invalidWindowsComponent(component string) bool {
	if component == "" || component == "." {
		return false
	}
	for _, character := range component {
		if character < 32 || strings.ContainsRune(`<>"|?*`, character) {
			return true
		}
	}
	if strings.HasSuffix(component, " ") || strings.HasSuffix(component, ".") {
		return true
	}
	base := strings.ToUpper(strings.SplitN(component, ".", 2)[0])
	if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || base == "CONIN$" || base == "CONOUT$" {
		return true
	}
	if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9' {
		return true
	}
	if strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT") {
		suffix := strings.TrimPrefix(strings.TrimPrefix(base, "COM"), "LPT")
		if suffix == "¹" || suffix == "²" || suffix == "³" {
			return true
		}
	}
	return false
}

// resolveExisting relies on os.Root for race-safe confinement. It may follow a
// link that remains inside the root, but can never follow one outside it.
func (s *FilesystemServer) resolveExisting(logical string) (string, os.FileInfo, error) {
	current := filepath.FromSlash(logical)
	if logical == "" {
		current = "."
		info, err := s.rootHandle.Stat(current)
		return current, info, err
	}
	info, err := s.rootHandle.Stat(current)
	return current, info, err
}

// prepareWritePath creates parents through os.Root, which confines any link
// traversal to the already-open workspace root.
func (s *FilesystemServer) prepareWritePath(logical string) (string, error) {
	destination := filepath.FromSlash(logical)
	if err := s.rootHandle.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return "", filesystemError("create workspace directory", err)
	}
	return destination, nil
}

func (s *FilesystemServer) createStagingFile(destination string) (*os.File, string, error) {
	directory := filepath.Dir(destination)
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", status.Errorf(codes.Internal, "generate staging name: %v", err)
		}
		name := filepath.Join(directory, ".sandboxd-write-"+hex.EncodeToString(random[:]))
		file, err := s.rootHandle.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", filesystemError("create staging file", err)
		}
		return file, name, nil
	}
	return nil, "", status.Error(codes.ResourceExhausted, "could not allocate a staging name")
}

func filesystemError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.NotFound, "%s: %v", operation, err)
	}
	if errors.Is(err, os.ErrExist) {
		return status.Errorf(codes.FailedPrecondition, "%s: path component has an incompatible type", operation)
	}
	if errors.Is(err, os.ErrPermission) {
		return status.Errorf(codes.PermissionDenied, "%s: %v", operation, err)
	}
	if errors.Is(err, os.ErrInvalid) || pathEscapesRoot(err) {
		return status.Errorf(codes.FailedPrecondition, "%s: path is not confined to the workspace", operation)
	}
	return status.Errorf(codes.Internal, "%s: %v", operation, err)
}

func pathEscapesRoot(err error) bool {
	for err != nil {
		// os.Root's confinement sentinel is intentionally unexported.
		if err.Error() == "path escapes from parent" {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func hashFile(ctx context.Context, file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	hash := sha256.New()
	buf := make([]byte, filesystemHashChunk)
	for {
		if err := contextError(ctx); err != nil {
			return "", err
		}
		n, err := file.Read(buf)
		if n > 0 {
			_, _ = hash.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			return hex.EncodeToString(hash.Sum(nil)), nil
		}
		if err != nil {
			return "", err
		}
	}
}

func (s *FilesystemServer) Read(ctx context.Context, req *sandboxdv1.ReadRequest) (*sandboxdv1.ReadResponse, error) {
	if err := s.initialize(); err != nil {
		return nil, status.Errorf(codes.Internal, "initialize filesystem: %v", err)
	}
	logical, err := normalizeWorkspacePath(req.GetPath(), false)
	if err != nil {
		return nil, err
	}
	limit := s.MaxReadBytes
	if limit == 0 {
		limit = defaultMaxReadBytes
	}
	maxBytes := req.GetMaxBytes()
	if maxBytes == 0 {
		maxBytes = min(uint32(defaultReadBytes), limit)
	}
	if maxBytes > limit {
		return nil, status.Errorf(codes.InvalidArgument, "max_bytes exceeds %d", limit)
	}

	s.commitMu.RLock()
	defer s.commitMu.RUnlock()
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	file, err := s.rootHandle.Open(filepath.FromSlash(logical))
	if err != nil {
		return nil, filesystemError("open workspace file", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, filesystemError("inspect workspace file", err)
	}
	if !info.Mode().IsRegular() {
		return nil, status.Error(codes.FailedPrecondition, "read path is not a regular file")
	}
	size := uint64(info.Size())
	if req.GetOffset() > size {
		return nil, status.Errorf(codes.OutOfRange, "offset %d exceeds file size %d", req.GetOffset(), size)
	}
	version := ""
	if req.GetIncludeVersion() {
		version, err = hashFile(ctx, file)
		if err != nil {
			if status.Code(err) != codes.Unknown {
				return nil, err
			}
			return nil, filesystemError("hash workspace file", err)
		}
	}
	if _, err := file.Seek(int64(req.GetOffset()), io.SeekStart); err != nil {
		return nil, filesystemError("seek workspace file", err)
	}
	want := min(uint64(maxBytes), size-req.GetOffset())
	data := make([]byte, int(want))
	n, err := io.ReadFull(file, data)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, status.Error(codes.Aborted, "workspace file changed during read")
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, filesystemError("read workspace file", err)
	}
	data = data[:n]
	next := req.GetOffset() + uint64(len(data))
	return &sandboxdv1.ReadResponse{Data: data, Offset: req.GetOffset(), NextOffset: next, Size: size, Eof: next == size, Version: version}, nil
}

func (s *FilesystemServer) Write(stream sandboxdv1.FilesystemService_WriteServer) error {
	if err := s.initialize(); err != nil {
		return status.Errorf(codes.Internal, "initialize filesystem: %v", err)
	}
	select {
	case s.writeSlots <- struct{}{}:
		defer func() { <-s.writeSlots }()
	default:
		return status.Error(codes.ResourceExhausted, "too many concurrent writes")
	}
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		if ctxErr := contextError(ctx); ctxErr != nil {
			return ctxErr
		}
		if status.Code(err) != codes.Unknown {
			return err
		}
		return status.Errorf(codes.InvalidArgument, "write header: %v", err)
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first write message must be a header")
	}
	logical, err := normalizeWorkspacePath(header.GetPath(), false)
	if err != nil {
		return err
	}
	if err := validateWriteHeader(header); err != nil {
		return err
	}

	s.commitMu.Lock()
	destination, err := s.prepareWritePath(logical)
	if err == nil {
		if info, statErr := s.rootHandle.Lstat(destination); statErr == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				err = status.Error(codes.FailedPrecondition, "write destination is not a regular file")
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			err = filesystemError("inspect write destination", statErr)
		}
	}
	var staged *os.File
	var stagedName string
	if err == nil {
		staged, stagedName, err = s.createStagingFile(destination)
	}
	s.commitMu.Unlock()
	if err != nil {
		return err
	}
	defer s.rootHandle.Remove(stagedName)
	defer staged.Close()

	hash := sha256.New()
	var size uint64
	chunkLimit := s.MaxWriteChunkBytes
	if chunkLimit == 0 {
		chunkLimit = defaultMaxWriteChunk
	}
	fileLimit := s.MaxWriteBytes
	if fileLimit == 0 {
		fileLimit = defaultMaxWriteBytes
	}
	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if ctxErr := contextError(ctx); ctxErr != nil {
				return ctxErr
			}
			if status.Code(recvErr) != codes.Unknown {
				return recvErr
			}
			return status.Errorf(codes.Unavailable, "write stream interrupted: %v", recvErr)
		}
		if err := contextError(ctx); err != nil {
			return err
		}
		if message.GetHeader() != nil || message.GetData() == nil {
			return status.Error(codes.InvalidArgument, "write data messages must follow the single header")
		}
		data := message.GetData()
		if uint64(len(data)) > uint64(chunkLimit) {
			return status.Errorf(codes.ResourceExhausted, "write chunk exceeds %d bytes", chunkLimit)
		}
		if uint64(len(data)) > fileLimit-size {
			return status.Errorf(codes.ResourceExhausted, "write exceeds %d bytes", fileLimit)
		}
		if _, err := staged.Write(data); err != nil {
			return filesystemError("stage workspace file", err)
		}
		_, _ = hash.Write(data)
		size += uint64(len(data))
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := staged.Sync(); err != nil {
		return filesystemError("flush staging file", err)
	}
	if err := staged.Close(); err != nil {
		return filesystemError("close staging file", err)
	}
	version := hex.EncodeToString(hash.Sum(nil))

	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	destination, err = s.prepareWritePath(logical)
	if err != nil {
		return err
	}
	if err := s.checkWritePrecondition(ctx, destination, header); err != nil {
		return err
	}
	if header.GetPrecondition() == sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MUST_NOT_EXIST {
		if err := s.rootHandle.Link(stagedName, destination); err != nil {
			if errors.Is(err, os.ErrExist) {
				return status.Error(codes.FailedPrecondition, "write destination already exists")
			}
			return filesystemError("commit new workspace file", err)
		}
	} else if err := s.rootHandle.Rename(stagedName, destination); err != nil {
		return filesystemError("commit workspace file", err)
	}
	return stream.SendAndClose(&sandboxdv1.WriteResponse{Size: size, Version: version})
}

func validateWriteHeader(header *sandboxdv1.WriteHeader) error {
	switch header.GetPrecondition() {
	case sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY, sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MUST_NOT_EXIST:
		if header.GetExpectedVersion() != "" {
			return status.Error(codes.InvalidArgument, "expected_version is only valid with MATCH_VERSION")
		}
	case sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MATCH_VERSION:
		value := header.GetExpectedVersion()
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
			return status.Error(codes.InvalidArgument, "expected_version must be a lowercase SHA-256 digest")
		}
	default:
		return status.Error(codes.InvalidArgument, "write precondition must be specified")
	}
	return nil
}

func (s *FilesystemServer) checkWritePrecondition(ctx context.Context, destination string, header *sandboxdv1.WriteHeader) error {
	info, err := s.rootHandle.Lstat(destination)
	exists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return filesystemError("inspect write destination", err)
	}
	if exists && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return status.Error(codes.FailedPrecondition, "write destination is not a regular file")
	}
	switch header.GetPrecondition() {
	case sandboxdv1.WritePrecondition_WRITE_PRECONDITION_ANY:
		return nil
	case sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MUST_NOT_EXIST:
		if exists {
			return status.Error(codes.FailedPrecondition, "write destination already exists")
		}
		return nil
	case sandboxdv1.WritePrecondition_WRITE_PRECONDITION_MATCH_VERSION:
		if !exists {
			return status.Error(codes.FailedPrecondition, "write destination does not exist")
		}
		file, err := s.rootHandle.Open(destination)
		if err != nil {
			return filesystemError("open write destination", err)
		}
		defer file.Close()
		version, err := hashFile(ctx, file)
		if err != nil {
			if status.Code(err) != codes.Unknown {
				return err
			}
			return filesystemError("hash write destination", err)
		}
		if version != header.GetExpectedVersion() {
			return status.Error(codes.FailedPrecondition, "write destination version changed")
		}
		return nil
	default:
		panic("validated write precondition changed")
	}
}

func (s *FilesystemServer) List(ctx context.Context, req *sandboxdv1.ListRequest) (*sandboxdv1.ListResponse, error) {
	if err := s.initialize(); err != nil {
		return nil, status.Errorf(codes.Internal, "initialize filesystem: %v", err)
	}
	logical, err := normalizeWorkspacePath(req.GetPath(), true)
	if err != nil {
		return nil, err
	}
	limit := s.MaxPageSize
	if limit == 0 {
		limit = defaultMaxPageSize
	}
	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = min(uint32(defaultPageSize), limit)
	}
	if pageSize > limit {
		return nil, status.Errorf(codes.InvalidArgument, "page_size exceeds %d", limit)
	}
	offset, err := decodePageToken(logical, req.GetPageToken())
	if err != nil {
		return nil, err
	}

	s.commitMu.RLock()
	defer s.commitMu.RUnlock()
	hostPath, info, err := s.resolveExisting(logical)
	if err != nil {
		return nil, filesystemError("list workspace directory", err)
	}
	if !info.IsDir() {
		return nil, status.Error(codes.FailedPrecondition, "list path is not a directory")
	}
	directory, err := s.rootHandle.Open(hostPath)
	if err != nil {
		return nil, filesystemError("open workspace directory", err)
	}
	defer directory.Close()
	remaining := offset
	for remaining > 0 {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		step := min(remaining, uint64(defaultPageSize))
		names, readErr := directory.Readdirnames(int(step))
		remaining -= uint64(len(names))
		if errors.Is(readErr, io.EOF) && remaining > 0 {
			return nil, status.Error(codes.InvalidArgument, "page token is past the end of the directory")
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, filesystemError("scan workspace directory", readErr)
		}
	}
	infos, readErr := directory.Readdir(int(pageSize) + 1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, filesystemError("scan workspace directory", readErr)
	}
	more := len(infos) > int(pageSize)
	if more {
		infos = infos[:pageSize]
	}
	consumed := uint64(len(infos))
	entries := make([]*sandboxdv1.Entry, 0, len(infos))
	for _, child := range infos {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if strings.HasPrefix(child.Name(), ".sandboxd-write-") {
			continue
		}
		entryType := sandboxdv1.EntryType_ENTRY_TYPE_OTHER
		switch {
		case child.Mode()&os.ModeSymlink != 0:
			entryType = sandboxdv1.EntryType_ENTRY_TYPE_SYMLINK
		case child.Mode().IsRegular():
			entryType = sandboxdv1.EntryType_ENTRY_TYPE_FILE
		case child.IsDir():
			entryType = sandboxdv1.EntryType_ENTRY_TYPE_DIRECTORY
		}
		size := uint64(0)
		if entryType == sandboxdv1.EntryType_ENTRY_TYPE_FILE && child.Size() > 0 {
			size = uint64(child.Size())
		}
		entries = append(entries, &sandboxdv1.Entry{Name: child.Name(), Type: entryType, Size: size, ModifiedUnixMs: child.ModTime().UnixMilli()})
	}
	response := &sandboxdv1.ListResponse{Entries: entries}
	if more {
		response.NextPageToken = encodePageToken(logical, offset+consumed)
	}
	return response, nil
}

func pageTokenPrefix(logical string) string {
	sum := sha256.Sum256([]byte(logical))
	return hex.EncodeToString(sum[:8])
}

func encodePageToken(logical string, offset uint64) string {
	plain := pageTokenPrefix(logical) + ":" + strconv.FormatUint(offset, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(plain))
}

func decodePageToken(logical, token string) (uint64, error) {
	if token == "" {
		return 0, nil
	}
	if len(token) > 128 {
		return 0, status.Error(codes.InvalidArgument, "invalid page token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, "invalid page token")
	}
	prefix, value, found := strings.Cut(string(decoded), ":")
	if !found || prefix != pageTokenPrefix(logical) {
		return 0, status.Error(codes.InvalidArgument, "page token does not match path")
	}
	offset, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, "invalid page token")
	}
	return offset, nil
}
