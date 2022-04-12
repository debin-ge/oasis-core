package tendermint

import (
	"context"
	"fmt"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/oasisprotocol/oasis-core/go/common/identity"
	consensusAPI "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/common"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/full"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/seed"
	genesisAPI "github.com/oasisprotocol/oasis-core/go/genesis/api"
	upgradeAPI "github.com/oasisprotocol/oasis-core/go/upgrade/api"
)

const (
	// CfgMode configures the consensus backend mode.
	CfgMode = "consensus.tendermint.mode"
)

// Flags has the configuration flags.
var Flags = flag.NewFlagSet("", flag.ContinueOnError)

// New creates a new Tendermint consensus backend.
func New(
	ctx context.Context,
	dataDir string,
	identity *identity.Identity,
	upgrader upgradeAPI.Backend,
	genesisProvider genesisAPI.Provider,
) (consensusAPI.Backend, error) {
	switch mode := viper.GetString(CfgMode); mode {
	case consensusAPI.ModeFull:
		// Full node.
		return full.New(ctx, dataDir, identity, upgrader, genesisProvider)
	case consensusAPI.ModeSeed:
		// Seed-only node.
		return seed.New(dataDir, identity, genesisProvider)
	case consensusAPI.ModeArchive:
		// Archive node.
		return full.NewArchive(ctx, dataDir, identity, genesisProvider)
	default:
		return nil, fmt.Errorf("tendermint: unsupported mode: %s", mode)
	}
}

// Mode returns the configured tendermint mode.
func Mode() string {
	return viper.GetString(CfgMode)
}

func init() {
	Flags.String(CfgMode, consensusAPI.ModeFull, "tendermint mode (full, seed, archive)")

	_ = viper.BindPFlags(Flags)
	Flags.AddFlagSet(common.Flags)
	Flags.AddFlagSet(full.Flags)
}
