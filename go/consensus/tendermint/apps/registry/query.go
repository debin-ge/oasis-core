package registry

import (
	"context"
	"fmt"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/entity"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	abciAPI "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	registryState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry/state"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

// Query is the registry query interface.
type Query interface {
	Entity(context.Context, staking.Address) (*entity.Entity, error)
	Entities(context.Context) ([]*entity.Entity, error)
	Node(context.Context, signature.PublicKey) (*node.Node, error)
	NodeByConsensusAddress(context.Context, []byte) (*node.Node, error)
	NodeStatus(context.Context, signature.PublicKey) (*registry.NodeStatus, error)
	Nodes(context.Context) ([]*node.Node, error)
	Runtime(context.Context, common.Namespace) (*registry.Runtime, error)
	Runtimes(ctx context.Context, includeSuspended bool) ([]*registry.Runtime, error)
	Genesis(context.Context) (*registry.Genesis, error)
}

// QueryFactory is the registry query factory.
type QueryFactory struct {
	state abciAPI.ApplicationQueryState
}

// QueryAt returns the registry query interface for a specific height.
func (sf *QueryFactory) QueryAt(ctx context.Context, height int64) (Query, error) {
	state, err := registryState.NewImmutableState(ctx, sf.state, height)
	if err != nil {
		return nil, err
	}
	return &registryQuerier{sf.state, state, height}, nil
}

type registryQuerier struct {
	queryState abciAPI.ApplicationQueryState
	state      *registryState.ImmutableState
	height     int64
}

func (rq *registryQuerier) Entity(ctx context.Context, address staking.Address) (*entity.Entity, error) {
	return rq.state.Entity(ctx, address)
}

func (rq *registryQuerier) Entities(ctx context.Context) ([]*entity.Entity, error) {
	return rq.state.Entities(ctx)
}

func (rq *registryQuerier) Node(ctx context.Context, id signature.PublicKey) (*node.Node, error) {
	epoch, err := rq.queryState.GetEpoch(ctx, rq.height)
	if err != nil {
		return nil, fmt.Errorf("failed to get epoch: %w", err)
	}

	node, err := rq.state.Node(ctx, id)
	if err != nil {
		return nil, err
	}

	// Do not return expired nodes.
	if node.IsExpired(uint64(epoch)) {
		return nil, registry.ErrNoSuchNode
	}
	return node, nil
}

func (rq *registryQuerier) NodeByConsensusAddress(ctx context.Context, address []byte) (*node.Node, error) {
	return rq.state.NodeByConsensusAddress(ctx, address)
}

func (rq *registryQuerier) NodeStatus(ctx context.Context, id signature.PublicKey) (*registry.NodeStatus, error) {
	return rq.state.NodeStatus(ctx, id)
}

func (rq *registryQuerier) Nodes(ctx context.Context) ([]*node.Node, error) {
	epoch, err := rq.queryState.GetEpoch(ctx, rq.height)
	if err != nil {
		return nil, fmt.Errorf("failed to get epoch: %w", err)
	}

	nodes, err := rq.state.Nodes(ctx)
	if err != nil {
		return nil, err
	}

	// Filter out expired nodes.
	var filteredNodes []*node.Node
	for _, n := range nodes {
		if n.IsExpired(uint64(epoch)) {
			continue
		}
		filteredNodes = append(filteredNodes, n)
	}
	return filteredNodes, nil
}

func (rq *registryQuerier) Runtime(ctx context.Context, id common.Namespace) (*registry.Runtime, error) {
	return rq.state.Runtime(ctx, id)
}

func (rq *registryQuerier) Runtimes(ctx context.Context, includeSuspended bool) ([]*registry.Runtime, error) {
	if includeSuspended {
		return rq.state.AllRuntimes(ctx)
	}
	return rq.state.Runtimes(ctx)
}

func (app *registryApplication) QueryFactory() interface{} {
	return &QueryFactory{app.state}
}

// NewQueryFactory returns a new QueryFactory backed by the given state
// instance.
func NewQueryFactory(state abciAPI.ApplicationQueryState) *QueryFactory {
	return &QueryFactory{state}
}
