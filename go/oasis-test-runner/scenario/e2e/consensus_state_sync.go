package e2e

import (
	"context"
	"fmt"
	"time"

	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/env"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/log"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/oasis"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/scenario"
)

// stateCheckpointInterval is the state checkpoint interval.
const stateCheckpointInterval = 10

// ConsensusStateSync is the consensus state sync scenario.
var ConsensusStateSync scenario.Scenario = &consensusStateSyncImpl{
	E2E: *NewE2E("consensus-state-sync"),
}

type consensusStateSyncImpl struct {
	E2E
}

func (sc *consensusStateSyncImpl) Clone() scenario.Scenario {
	return &consensusStateSyncImpl{
		E2E: sc.E2E.Clone(),
	}
}

func (sc *consensusStateSyncImpl) Fixture() (*oasis.NetworkFixture, error) {
	f, err := sc.E2E.Fixture()
	if err != nil {
		return nil, err
	}

	f.Network.SetInsecureBeacon()

	// Enable checkpoints.
	f.Network.Consensus.Parameters.StateCheckpointInterval = stateCheckpointInterval
	f.Network.Consensus.Parameters.StateCheckpointNumKept = 100
	f.Network.Consensus.Parameters.StateCheckpointChunkSize = 1024 * 1024
	// Add an extra validator.
	f.Validators = append(f.Validators,
		oasis.ValidatorFixture{
			NodeFixture: oasis.NodeFixture{
				NoAutoStart: true,
			},
			Entity:    1,
			Consensus: oasis.ConsensusFixture{EnableConsensusRPCWorker: true},
			LogWatcherHandlerFactories: []log.WatcherHandlerFactory{
				oasis.LogEventABCIStateSyncComplete(),
			},
		},
	)
	// Disable certificate rotation on validator nodes so we can more easily use them for sync.
	for i := range f.Validators {
		f.Validators[i].DisableCertRotation = true
	}

	return f, nil
}

func (sc *consensusStateSyncImpl) Run(childEnv *env.Env) error {
	if err := sc.Net.Start(); err != nil {
		return err
	}

	sc.Logger.Info("waiting for network to come up")
	ctx := context.Background()
	if err := sc.Net.Controller().WaitNodesRegistered(ctx, len(sc.Net.Validators())-1); err != nil {
		return err
	}

	// Let the network run for 50 blocks. This should generate some checkpoints.
	blockCh, blockSub, err := sc.Net.Controller().Consensus.WatchBlocks(ctx)
	if err != nil {
		return err
	}
	defer blockSub.Close()

	sc.Logger.Info("waiting for some blocks")
	var blk *consensus.Block
	for {
		select {
		case blk = <-blockCh:
			if blk.Height < 50 {
				continue
			}
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timed out waiting for blocks")
		}

		break
	}

	sc.Logger.Info("got some blocks, starting the validator that needs to sync",
		"trust_height", blk.Height,
		"trust_hash", blk.Hash.Hex(),
	)

	// The last validator configured by the fixture is the one that is stopped and will sync.
	lastValidator := len(sc.Net.Validators()) - 1

	// Configure state sync for the consensus validator.
	val := sc.Net.Validators()[lastValidator]
	val.SetConsensusStateSync(&oasis.ConsensusStateSyncCfg{
		TrustHeight: uint64(blk.Height),
		TrustHash:   blk.Hash.Hex(),
	})

	if err = val.Start(); err != nil {
		return fmt.Errorf("failed to start validator: %w", err)
	}

	// Wait for the validator to finish syncing.
	sc.Logger.Info("waiting for the validator to sync")
	valCtrl, err := oasis.NewController(val.SocketPath())
	if err != nil {
		return err
	}
	if err = valCtrl.WaitSync(ctx); err != nil {
		return err
	}

	// Query the validator status.
	ctrl, err := oasis.NewController(val.SocketPath())
	if err != nil {
		return fmt.Errorf("failed to create controller for validator %s: %w", val.Name, err)
	}
	status, err := ctrl.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch validator status: %w", err)
	}
	if status.Consensus.Status != consensus.StatusStateReady {
		return fmt.Errorf("synced validator not ready")
	}

	// Make sure that the last retained height has been set correctly.
	// This should be at least stateCheckpointInterval or higher.
	if lrh := status.Consensus.LastRetainedHeight; lrh < stateCheckpointInterval {
		return fmt.Errorf("unexpected last retained height from state synced node (got: %d)", lrh)
	}

	return sc.Net.CheckLogWatchers()
}
