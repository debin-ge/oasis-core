package runtime

import (
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/oasis"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/scenario"
	"github.com/oasisprotocol/oasis-core/go/runtime/bundle/component"
)

// ROFL is the runtime with a ROFL component scenario.
var ROFL scenario.Scenario = newROFL()

type roflImpl struct {
	Scenario
}

func newROFL() scenario.Scenario {
	return &roflImpl{
		Scenario: *NewScenario("rofl", NewTestClient().WithScenario(NewTestClientScenario([]interface{}{
			InsertKeyValueTx{"my_key", "my_value", "", true, 0},
			GetKeyValueTx{"my_key", "my_value", true, 0},
			RemoveKeyValueTx{"my_key", "my_value", true, 0},
			GetKeyValueTx{"my_key", "", true, 0},
			// Check that the ROFL component wrote the HTTP response into storage.
			KeyExistsTx{"rofl_http", false, 0},
		}))),
	}
}

func (sc *roflImpl) Clone() scenario.Scenario {
	return &roflImpl{
		Scenario: *sc.Scenario.Clone().(*Scenario),
	}
}

func (sc *roflImpl) Fixture() (*oasis.NetworkFixture, error) {
	f, err := sc.Scenario.Fixture()
	if err != nil {
		return nil, err
	}

	// Add ROFL component.
	f.Runtimes[1].Deployments[0].Components = append(f.Runtimes[1].Deployments[0].Components, oasis.ComponentCfg{
		Kind:     component.ROFL,
		Binaries: sc.ResolveRuntimeBinaries(ROFLComponentBinary),
	})

	return f, nil
}
