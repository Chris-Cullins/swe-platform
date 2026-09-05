package sandboxclient

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
	"google.golang.org/grpc"
)

// SnapshotChanges has a purpose-only credential and repeats the complete private
// backend/credential proof after the bounded RPC. It never uses public Exec.
func (c Connector) SnapshotChanges(ctx context.Context, fence lifecycle.ExecutionFence, baselinePaths []string) (changes.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	env, secret, proof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return changes.Snapshot{}, err
	}
	token := string(secret.Data[sandboxdauth.ChangesTokenKey])
	if !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], token, sandboxdauth.CapabilityChanges) {
		return changes.Snapshot{}, errors.New("changes capability unavailable")
	}
	capHash := sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey])
	opts, err := privateDialOptions(secret.Data[sandboxdauth.TLSCertKey], proof.identity, token)
	if err != nil {
		return changes.Snapshot{}, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, opts...)
	if err != nil {
		return changes.Snapshot{}, err
	}
	defer conn.Close()
	response, rpcErr := sandboxdv1.NewChangesServiceClient(conn).Snapshot(ctx, &sandboxdv1.ChangesSnapshotRequest{BaselinePaths: baselinePaths}, grpc.MaxCallRecvMsgSize(changes.MaxEncodedBytes+1024))
	_, currentSecret, current, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return changes.Snapshot{}, err
	}
	if !proof.matches(current) || capHash != sha256.Sum256(currentSecret.Data[sandboxdauth.CapabilitiesKey]) || token != string(currentSecret.Data[sandboxdauth.ChangesTokenKey]) {
		return changes.Snapshot{}, errors.New("execution changed during changes capture")
	}
	if rpcErr != nil {
		return changes.Snapshot{}, rpcErr
	}
	var snapshot changes.Snapshot
	if response == nil || len(response.SnapshotJson) > changes.MaxEncodedBytes {
		return snapshot, errors.New("invalid changes response")
	}
	if err = json.Unmarshal(response.SnapshotJson, &snapshot); err != nil {
		return changes.Snapshot{}, err
	}
	if err = snapshot.Validate(); err != nil {
		return changes.Snapshot{}, err
	}
	return snapshot, nil
}
