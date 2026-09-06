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

// ChangesObservation retains private execution authority for a publication-time
// recheck. Snapshot bytes themselves are never execution authority.
type ChangesObservation struct {
	Snapshot         changes.Snapshot
	proof            processConnectionProof
	capabilitiesHash [sha256.Size]byte
	tokenHash        [sha256.Size]byte
}

// SnapshotChanges has a purpose-only credential and repeats the complete private
// backend/credential proof after the bounded RPC. It never uses public Exec.
func (c Connector) SnapshotChanges(ctx context.Context, fence lifecycle.ExecutionFence, baselinePaths []string) (ChangesObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	env, secret, proof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return ChangesObservation{}, err
	}
	token := string(secret.Data[sandboxdauth.ChangesTokenKey])
	if !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], token, sandboxdauth.CapabilityChanges) {
		return ChangesObservation{}, errors.New("changes capability unavailable")
	}
	capHash := sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey])
	opts, err := privateDialOptions(secret.Data[sandboxdauth.TLSCertKey], proof.identity, token)
	if err != nil {
		return ChangesObservation{}, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, opts...)
	if err != nil {
		return ChangesObservation{}, err
	}
	defer conn.Close()
	response, rpcErr := sandboxdv1.NewChangesServiceClient(conn).Snapshot(ctx, &sandboxdv1.ChangesSnapshotRequest{BaselinePaths: baselinePaths}, grpc.MaxCallRecvMsgSize(changes.MaxEncodedBytes+1024))
	result := ChangesObservation{proof: proof, capabilitiesHash: capHash, tokenHash: sha256.Sum256([]byte(token))}
	if err := c.ChangesCurrent(ctx, fence, result); err != nil {
		return ChangesObservation{}, err
	}
	if rpcErr != nil {
		return ChangesObservation{}, rpcErr
	}
	var snapshot changes.Snapshot
	if response == nil || len(response.SnapshotJson) > changes.MaxEncodedBytes {
		return ChangesObservation{}, errors.New("invalid changes response")
	}
	if err = json.Unmarshal(response.SnapshotJson, &snapshot); err != nil {
		return ChangesObservation{}, err
	}
	if err = snapshot.Validate(); err != nil {
		return ChangesObservation{}, err
	}
	result.Snapshot = snapshot
	return result, nil
}

func (c Connector) ChangesCurrent(ctx context.Context, fence lifecycle.ExecutionFence, observation ChangesObservation) error {
	_, secret, current, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return err
	}
	token := string(secret.Data[sandboxdauth.ChangesTokenKey])
	if !observation.proof.matches(current) || observation.capabilitiesHash != sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]) || observation.tokenHash != sha256.Sum256([]byte(token)) || !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], token, sandboxdauth.CapabilityChanges) {
		return errors.New("execution changed during changes capture")
	}
	return nil
}
