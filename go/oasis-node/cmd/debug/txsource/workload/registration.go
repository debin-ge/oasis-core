package workload

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	memorySigner "github.com/oasisprotocol/oasis-core/go/common/crypto/signature/signers/memory"
	"github.com/oasisprotocol/oasis-core/go/common/entity"
	"github.com/oasisprotocol/oasis-core/go/common/identity"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/quantity"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	cmdCommon "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

// NameRegistration is the name of the registration workload.
const NameRegistration = "registration"

// Registration is the registration workload.
var Registration = &registration{
	BaseWorkload: NewBaseWorkload(NameRegistration),
}

const (
	registryNumEntities           = 10
	registryNumNodesPerEntity     = 5
	registryNodeMaxEpochUpdate    = 5
	registryRtOwnerChangeInterval = 20

	registryIterationTimeout = 15 * time.Second
)

type registration struct {
	BaseWorkload

	ns common.Namespace
}

func getRuntime(entityID signature.PublicKey, id common.Namespace, epoch beacon.EpochTime) *registry.Runtime {
	rt := &registry.Runtime{
		Versioned: cbor.NewVersioned(registry.LatestRuntimeDescriptorVersion),
		ID:        id,
		EntityID:  entityID,
		Kind:      registry.KindCompute,
		Executor: registry.ExecutorParameters{
			GroupSize:    1,
			RoundTimeout: 5,
			MaxMessages:  32,
		},
		TxnScheduler: registry.TxnSchedulerParameters{
			BatchFlushTimeout: 1 * time.Second,
			MaxBatchSize:      1,
			MaxBatchSizeBytes: 1024,
			ProposerTimeout:   5,
		},
		AdmissionPolicy: registry.RuntimeAdmissionPolicy{
			AnyNode: &registry.AnyNodeRuntimeAdmissionPolicy{},
		},
		Constraints: map[scheduler.CommitteeKind]map[scheduler.Role]registry.SchedulingConstraints{
			scheduler.KindComputeExecutor: {
				scheduler.RoleWorker: {
					MinPoolSize: &registry.MinPoolSizeConstraint{
						Limit: 1,
					},
				},
			},
		},
		GovernanceModel: registry.GovernanceEntity,
		Deployments: []*registry.VersionInfo{
			{
				ValidFrom: epoch + 1,
			},
		},
	}
	rt.Genesis.StateRoot.Empty()
	return rt
}

func getNodeDesc(rng *rand.Rand, nodeIdentity *identity.Identity, entityID signature.PublicKey, runtimeID common.Namespace) *node.Node {
	nodeAddr := node.Address{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 12345,
		Zone: "",
	}

	// NOTE: we shouldn't be registering validators, as that would lead to
	// consensus stopping as the registered validators wouldn't actually
	// exist.
	availableRoles := []node.RolesMask{
		node.RoleComputeWorker,
	}

	nodeDesc := node.Node{
		Versioned:  cbor.NewVersioned(node.LatestNodeDescriptorVersion),
		ID:         nodeIdentity.NodeSigner.Public(),
		EntityID:   entityID,
		Expiration: 0,
		Roles:      availableRoles[rng.Intn(len(availableRoles))],
		TLS: node.TLSInfo{
			PubKey: nodeIdentity.GetTLSSigner().Public(),
		},
		P2P: node.P2PInfo{
			ID: nodeIdentity.P2PSigner.Public(),
			Addresses: []node.Address{
				nodeAddr,
			},
		},
		Consensus: node.ConsensusInfo{
			ID: nodeIdentity.ConsensusSigner.Public(),
			Addresses: []node.ConsensusAddress{
				{
					ID:      nodeIdentity.P2PSigner.Public(),
					Address: nodeAddr,
				},
			},
		},
		Runtimes: []*node.Runtime{
			{
				ID: runtimeID,
			},
		},
		VRF: &node.VRFInfo{
			ID: nodeIdentity.VRFSigner.Public(),
		},
	}
	return &nodeDesc
}

func signNode(identity *identity.Identity, nodeDesc *node.Node) (*node.MultiSignedNode, error) {
	return node.MultiSignNode(
		[]signature.Signer{
			identity.NodeSigner,
			identity.P2PSigner,
			identity.ConsensusSigner,
			identity.GetTLSSigner(),
			identity.VRFSigner,
		},
		registry.RegisterNodeSignatureContext,
		nodeDesc,
	)
}

// Implements Workload.
func (r *registration) NeedsFunds() bool {
	return true
}

// Implements Workload.
func (r *registration) Run( // nolint: gocyclo
	gracefulExit context.Context,
	rng *rand.Rand,
	conn *grpc.ClientConn,
	cnsc consensus.ClientBackend,
	sm consensus.SubmissionManager,
	fundingAccount signature.Signer,
	validatorEntities []signature.Signer,
) error {
	// Initialize base workload.
	r.BaseWorkload.Init(cnsc, sm, fundingAccount)

	beacon := beacon.NewBeaconClient(conn)
	ctx := context.Background()
	var err error

	// Non-existing runtime.
	if err = r.ns.UnmarshalHex("0000000000000000000000000000000000000000000000000000000000000002"); err != nil {
		panic(err)
	}

	baseDir := viper.GetString(cmdCommon.CfgDataDir)
	nodeIdentitiesDir := filepath.Join(baseDir, "node-identities")
	if err = common.Mkdir(nodeIdentitiesDir); err != nil {
		return fmt.Errorf("txsource/registration: failed to create node-identities dir: %w", err)
	}

	type runtimeInfo struct {
		entityIdx int
		desc      *registry.Runtime
	}
	rtInfo := &runtimeInfo{}

	// Load all accounts.
	type nodeAcc struct {
		id            *identity.Identity
		nodeDesc      *node.Node
		reckonedNonce uint64
	}
	entityAccs := make([]struct {
		signer         signature.Signer
		address        staking.Address
		reckonedNonce  uint64
		nodeIdentities []*nodeAcc
	}, registryNumEntities)

	fac := memorySigner.NewFactory()
	for i := range entityAccs {
		entityAccs[i].signer, err = fac.Generate(signature.SignerEntity, rng)
		if err != nil {
			return fmt.Errorf("memory signer factory Generate account %d: %w", i, err)
		}
		entityAccs[i].address = staking.NewAddress(entityAccs[i].signer.Public())
	}

	// Register entities.
	// XXX: currently entities are only registered at start. Could also
	// periodically register new entities.
	for i := range entityAccs {
		entityAccs[i].reckonedNonce, err = cnsc.GetSignerNonce(ctx, &consensus.GetSignerNonceRequest{
			AccountAddress: entityAccs[i].address,
			Height:         consensus.HeightLatest,
		})
		if err != nil {
			return fmt.Errorf("GetSignerNonce error: %w", err)
		}

		ent := &entity.Entity{
			Versioned: cbor.NewVersioned(entity.LatestDescriptorVersion),
			ID:        entityAccs[i].signer.Public(),
		}

		// Generate entity node identities.
		for j := 0; j < registryNumNodesPerEntity; j++ {
			dataDir, err := os.MkdirTemp(nodeIdentitiesDir, "node_")
			if err != nil {
				return fmt.Errorf("failed to create a temporary directory: %w", err)
			}
			ident, err := identity.LoadOrGenerate(dataDir, memorySigner.NewFactory(), false)
			if err != nil {
				return fmt.Errorf("failed generating account node identity: %w", err)
			}
			nodeDesc := getNodeDesc(rng, ident, entityAccs[i].signer.Public(), r.ns)

			var nodeAccNonce uint64
			nodeAccAddress := staking.NewAddress(ident.NodeSigner.Public())
			nodeAccNonce, err = cnsc.GetSignerNonce(ctx, &consensus.GetSignerNonceRequest{
				AccountAddress: nodeAccAddress,
				Height:         consensus.HeightLatest,
			})
			if err != nil {
				return fmt.Errorf("GetSignerNonce error for accout %s: %w", nodeAccAddress, err)
			}

			entityAccs[i].nodeIdentities = append(entityAccs[i].nodeIdentities, &nodeAcc{ident, nodeDesc, nodeAccNonce})
			ent.Nodes = append(ent.Nodes, ident.NodeSigner.Public())
		}

		// Register entity.
		sigEntity, err := entity.SignEntity(entityAccs[i].signer, registry.RegisterEntitySignatureContext, ent)
		if err != nil {
			return fmt.Errorf("failed to sign entity: %w", err)
		}

		// Submit register entity transaction.
		tx := registry.NewRegisterEntityTx(entityAccs[i].reckonedNonce, nil, sigEntity)
		entityAccs[i].reckonedNonce++
		if err = r.FundSignAndSubmitTx(ctx, entityAccs[i].signer, tx); err != nil {
			r.Logger.Error("failed to sign and submit regsiter entity transaction",
				"tx", tx,
				"signer", entityAccs[i].signer,
			)
			return fmt.Errorf("failed to sign and submit tx: %w", err)
		}

		// Ensure entities have required stake to register runtime.
		if err = r.EscrowFunds(ctx, fundingAccount, entityAccs[i].address, quantity.NewFromUint64(10_000)); err != nil {
			return fmt.Errorf("account escrow failure: %w", err)
		}

		// Register runtime.
		// XXX: currently only a single runtime is used throughout the test, could use more.
		if i == 0 {
			// Current epoch.
			epoch, err := beacon.GetEpoch(ctx, consensus.HeightLatest)
			if err != nil {
				return fmt.Errorf("failed to get current epoch: %w", err)
			}

			rtInfo.entityIdx = i
			rtInfo.desc = getRuntime(entityAccs[i].signer.Public(), r.ns, epoch)
			tx := registry.NewRegisterRuntimeTx(entityAccs[i].reckonedNonce, nil, rtInfo.desc)
			entityAccs[i].reckonedNonce++
			if err := r.FundSignAndSubmitTx(ctx, entityAccs[i].signer, tx); err != nil {
				r.Logger.Error("failed to sign and submit register runtime transaction",
					"tx", tx,
					"signer", entityAccs[i].signer,
				)
				return fmt.Errorf("failed to sign and submit tx: %w", err)
			}
		}
	}

	iteration := 0
	var loopCtx context.Context
	var cancel context.CancelFunc
	for {
		if cancel != nil {
			cancel()
		}
		loopCtx, cancel = context.WithTimeout(ctx, registryIterationTimeout)
		defer cancel()

		// Select a random node from random entity and register it.
		selectedEntityIdx := rng.Intn(registryNumEntities)
		selectedAcc := &entityAccs[selectedEntityIdx]
		selectedNode := selectedAcc.nodeIdentities[rng.Intn(registryNumNodesPerEntity)]

		// Current epoch.
		epoch, err := beacon.GetEpoch(loopCtx, consensus.HeightLatest)
		if err != nil {
			return fmt.Errorf("failed to get current epoch: %w", err)
		}

		// Randomized expiration.
		// We should update for at minimum 2 epochs, as the epoch could change between querying it
		// and actually performing the registration.
		selectedNode.nodeDesc.Expiration = uint64(epoch) + 2 + uint64(rng.Intn(registryNodeMaxEpochUpdate-1))
		sigNode, err := signNode(selectedNode.id, selectedNode.nodeDesc)
		if err != nil {
			return fmt.Errorf("failed to sign node: %w", err)
		}

		// Register node.
		tx := registry.NewRegisterNodeTx(selectedNode.reckonedNonce, nil, sigNode)
		selectedNode.reckonedNonce++
		if err := r.FundSignAndSubmitTx(loopCtx, selectedNode.id.NodeSigner, tx); err != nil {
			r.Logger.Error("failed to sign and submit register node transaction",
				"tx", tx,
				"signer", selectedNode.id.NodeSigner,
			)
			return fmt.Errorf("failed to sign and submit tx: %w", err)
		}

		r.Logger.Debug("registered node",
			"node", selectedNode.nodeDesc,
		)

		// Periodically re-register the runtime with a new owner.
		if iteration&registryRtOwnerChangeInterval == 0 {
			// Update runtime owner.
			currentOwner := rtInfo.entityIdx
			rtInfo.desc.EntityID = entityAccs[selectedEntityIdx].signer.Public()
			rtInfo.entityIdx = selectedEntityIdx
			// Sign the transaction with current owner.
			tx := registry.NewRegisterRuntimeTx(entityAccs[currentOwner].reckonedNonce, nil, rtInfo.desc)
			entityAccs[currentOwner].reckonedNonce++

			if err := r.FundSignAndSubmitTx(loopCtx, entityAccs[currentOwner].signer, tx); err != nil {
				r.Logger.Error("failed to sign and submit register runtime transaction",
					"tx", tx,
					"signer", entityAccs[currentOwner].signer,
				)
				return fmt.Errorf("failed to sign and submit tx: %w", err)
			}

			r.Logger.Debug("registered runtime",
				"runtime", rtInfo.desc,
			)
		}

		iteration++
		select {
		case <-time.After(1 * time.Second):
		case <-gracefulExit.Done():
			r.Logger.Debug("time's up")
			return nil
		}
	}
}
