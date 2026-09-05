package auth

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"testing"
)

func TestChangesCapabilityHasNoWriteOrExecAuthority(t *testing.T) {
	a := newTestAuthorizer(t, Config{Grants: []Grant{{TokenHash: TokenVerifier("changes"), Capabilities: []Capability{CapabilityChanges}}, {TokenHash: TokenVerifier("process"), Capabilities: []Capability{CapabilityProcess}}}})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer changes"))
	if err := a.authorize(ctx, "/sandboxd.v1.ChangesService/Snapshot"); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"ExecService/Exec", "ProcessService/Start", "FilesystemService/Read", "FilesystemService/Write", "TerminalService/Terminal"} {
		if err := a.authorize(ctx, "/sandboxd.v1."+method); status.Code(err) != codes.PermissionDenied {
			t.Fatal(method, err)
		}
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer process"))
	if err := a.authorize(ctx, "/sandboxd.v1.ChangesService/Snapshot"); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}
