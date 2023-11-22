// Package sgx implements the runtime provisioner for runtimes in Intel SGX enclaves.
package sgx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/persistent"
	"github.com/oasisprotocol/oasis-core/go/common/sgx"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/aesm"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/pcs"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/sigstruct"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	ias "github.com/oasisprotocol/oasis-core/go/ias/api"
	cmdFlags "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	"github.com/oasisprotocol/oasis-core/go/runtime/host"
	"github.com/oasisprotocol/oasis-core/go/runtime/host/protocol"
	"github.com/oasisprotocol/oasis-core/go/runtime/host/sandbox"
	"github.com/oasisprotocol/oasis-core/go/runtime/host/sandbox/process"
)

const (
	// TODO: Support different locations for the AESMD socket.
	aesmdSocketPath = "/var/run/aesmd/aesm.socket"

	sandboxMountRuntime   = "/runtime"
	sandboxMountSignature = "/runtime.sig"

	// The service name for the common store to use for SGX-related persistent data.
	serviceStoreName = "runtime_host_sgx"

	// Runtime RAK initialization timeout.
	//
	// This can take a long time in deployments that run multiple
	// nodes on a single machine, all sharing the same EPC.
	runtimeRAKTimeout = 60 * time.Second
	// Runtime attest interval.
	defaultRuntimeAttestInterval = 2 * time.Hour
)

// Key of the platform manifest in the common store.
var platformManifestKey = []byte("platform_manifest")

// Config contains SGX-specific provisioner configuration options.
type Config struct {
	// HostInfo provides information about the host environment.
	HostInfo *protocol.HostInfo

	// CommonStore is a handle to the node's common persistent store.
	CommonStore *persistent.CommonStore

	// LoaderPath is the path to the runtime loader binary.
	LoaderPath string

	// IAS are the Intel Attestation Service endpoint.
	IAS []ias.Endpoint
	// PCS is the Intel Provisioning Certification Service client.
	PCS pcs.Client
	// Consensus is the consensus layer backend.
	Consensus consensus.Backend

	// RuntimeAttestInterval is the interval for periodic runtime re-attestation. If not specified
	// a default will be used.
	RuntimeAttestInterval time.Duration

	// SandboxBinaryPath is the path to the sandbox support binary.
	SandboxBinaryPath string

	// InsecureNoSandbox disables the sandbox and runs the loader directly.
	InsecureNoSandbox bool
}

// RuntimeExtra is the extra configuration for SGX runtimes.
type RuntimeExtra struct {
	// SignaturePath is the path to the runtime (enclave) SIGSTRUCT.
	SignaturePath string

	// UnsafeDebugGenerateSigstruct allows the generation of a dummy SIGSTRUCT
	// if an actual signature is unavailable.
	UnsafeDebugGenerateSigstruct bool
}

type teeStateImpl interface {
	// Init initializes the TEE state and returns the QE target info.
	Init(ctx context.Context, sp *sgxProvisioner, runtimeID common.Namespace, version version.Version) ([]byte, error)

	// Update updates the TEE state and returns a new attestation.
	Update(ctx context.Context, sp *sgxProvisioner, conn protocol.Connection, report []byte, nonce string) ([]byte, error)
}

type teeState struct {
	runtimeID    common.Namespace
	version      version.Version
	eventEmitter host.RuntimeEventEmitter

	impl teeStateImpl
}

func (ts *teeState) init(ctx context.Context, sp *sgxProvisioner) ([]byte, error) {
	if ts.impl != nil {
		return nil, fmt.Errorf("already initialized")
	}

	var (
		targetInfo []byte
		err        error
	)

	// Try ECDSA first. If it fails, try EPID.
	implECDSA := &teeStateECDSA{}
	if targetInfo, err = implECDSA.Init(ctx, sp, ts.runtimeID, ts.version); err != nil {
		sp.logger.Debug("ECDSA attestation initialization failed, trying EPID",
			"err", err,
		)

		implEPID := &teeStateEPID{}
		if targetInfo, err = implEPID.Init(ctx, sp, ts.runtimeID, ts.version); err != nil {
			return nil, err
		}
		ts.impl = implEPID
	} else {
		ts.impl = implECDSA
	}

	return targetInfo, nil
}

func (ts *teeState) updateTargetInfo(ctx context.Context, sp *sgxProvisioner) ([]byte, error) {
	if ts.impl == nil {
		return nil, fmt.Errorf("not initialized")
	}
	return ts.impl.Init(ctx, sp, ts.runtimeID, ts.version)
}

func (ts *teeState) update(ctx context.Context, sp *sgxProvisioner, conn protocol.Connection, report []byte, nonce string) ([]byte, error) {
	if ts.impl == nil {
		return nil, fmt.Errorf("not initialized")
	}

	attestation, err := ts.impl.Update(ctx, sp, conn, report, nonce)

	updateAttestationMetrics(ts.runtimeID.String(), err)

	return attestation, err
}

type sgxProvisioner struct {
	sync.Mutex

	cfg Config

	sandbox   host.Provisioner
	ias       []ias.Endpoint
	pcs       pcs.Client
	aesm      *aesm.Client
	consensus consensus.Backend

	logger       *logging.Logger
	serviceStore *persistent.ServiceStore
}

func (s *sgxProvisioner) loadEnclaveBinaries(rtCfg host.Config) ([]byte, []byte, error) {
	var (
		sig, sgxs   []byte
		enclaveHash sgx.MrEnclave
		err         error
	)

	if sgxs, err = os.ReadFile(rtCfg.Bundle.Path); err != nil {
		return nil, nil, fmt.Errorf("failed to load enclave: %w", err)
	}
	if err = enclaveHash.FromSgxsBytes(sgxs); err != nil {
		return nil, nil, fmt.Errorf("failed to derive EnclaveHash: %w", err)
	}

	// If the path to an existing SIGSTRUCT is provided, load it.
	rtExtra, ok := rtCfg.Extra.(*RuntimeExtra)
	if !ok {
		return nil, nil, fmt.Errorf("sgx enclave configuration not available")
	}

	if rtExtra.SignaturePath != "" {
		sig, err = os.ReadFile(rtExtra.SignaturePath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load SIGSTRUCT: %w", err)
		}
	} else if rtExtra.UnsafeDebugGenerateSigstruct && cmdFlags.DebugDontBlameOasis() {
		s.logger.Warn("generating dummy enclave SIGSTRUCT",
			"enclave_hash", enclaveHash,
		)
		if sig, err = sigstruct.UnsafeDebugForEnclave(sgxs); err != nil {
			return nil, nil, fmt.Errorf("failed to generate debug SIGSTRUCT: %w", err)
		}
	} else {
		return nil, nil, fmt.Errorf("enclave SIGSTRUCT not available")
	}

	_, parsed, err := sigstruct.Verify(sig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to validate SIGSTRUCT: %w", err)
	}
	if parsed.EnclaveHash != enclaveHash {
		return nil, nil, fmt.Errorf("enclave/SIGSTRUCT mismatch")
	}

	return sgxs, sig, nil
}

func (s *sgxProvisioner) discoverSGXDevice() (string, error) {
	// Different versions of Intel SGX drivers provide different names for
	// the SGX device.  Autodetect which one actually exists.
	sgxDevices := []string{"/dev/sgx_enclave", "/dev/sgx/enclave", "/dev/sgx", "/dev/isgx"}
	for _, dev := range sgxDevices {
		fi, err := os.Stat(dev)
		if err != nil {
			continue
		}
		if fi.Mode()&os.ModeDevice != 0 {
			return dev, nil
		}
	}

	return "", fmt.Errorf("no SGX device was found on this system")
}

func (s *sgxProvisioner) getSandboxConfig(rtCfg host.Config, socketPath, runtimeDir string) (process.Config, error) {
	// To try to avoid bad things from happening if the signature/enclave
	// binaries change out from under us, and because the enclave binary
	// needs to be loaded into memory anyway, this always injects
	// (or copies).
	runtimePath, signaturePath := sandboxMountRuntime, sandboxMountSignature
	if s.cfg.InsecureNoSandbox {
		runtimePath = filepath.Join(runtimeDir, runtimePath)
		signaturePath = filepath.Join(runtimeDir, signaturePath)
	}

	sgxs, sig, err := s.loadEnclaveBinaries(rtCfg)
	if err != nil {
		return process.Config{}, fmt.Errorf("host/sgx: failed to load enclave/signature: %w", err)
	}

	sgxDev, err := s.discoverSGXDevice()
	if err != nil {
		return process.Config{}, fmt.Errorf("host/sgx: %w", err)
	}
	s.logger.Info("found SGX device", "path", sgxDev)

	logWrapper := host.NewRuntimeLogWrapper(
		s.logger,
		"runtime_id", rtCfg.Bundle.Manifest.ID,
		"runtime_name", rtCfg.Bundle.Manifest.Name,
	)

	return process.Config{
		Path: s.cfg.LoaderPath,
		Args: []string{
			"--host-socket", socketPath,
			"--type", "sgxs",
			"--signature", signaturePath,
			runtimePath,
		},
		BindRW: map[string]string{
			aesmdSocketPath: "/var/run/aesmd/aesm.socket",
		},
		BindDev: map[string]string{
			sgxDev: sgxDev,
		},
		BindData: map[string]io.Reader{
			runtimePath:   bytes.NewReader(sgxs),
			signaturePath: bytes.NewReader(sig),
		},
		SandboxBinaryPath: s.cfg.SandboxBinaryPath,
		Stdout:            logWrapper,
		Stderr:            logWrapper,
	}, nil
}

func (s *sgxProvisioner) hostInitializer(ctx context.Context, hp *sandbox.HostInitializerParams) (*host.StartedEvent, error) {
	// Initialize TEE.
	var err error
	var ts *teeState
	if ts, err = s.initCapabilityTEE(ctx, hp.Runtime, hp.Connection, hp.Version); err != nil {
		return nil, fmt.Errorf("failed to initialize TEE: %w", err)
	}
	var capabilityTEE *node.CapabilityTEE
	if capabilityTEE, err = s.updateCapabilityTEE(ctx, ts, hp.Connection); err != nil {
		return nil, fmt.Errorf("failed to initialize TEE: %w", err)
	}

	go s.attestationWorker(ts, hp)

	return &host.StartedEvent{
		Version:       hp.Version,
		CapabilityTEE: capabilityTEE,
	}, nil
}

func (s *sgxProvisioner) initCapabilityTEE(ctx context.Context, rt host.Runtime, conn protocol.Connection, version version.Version) (*teeState, error) {
	ctx, cancel := context.WithTimeout(ctx, runtimeRAKTimeout)
	defer cancel()

	ts := teeState{
		runtimeID: rt.ID(),
		version:   version,
		// We know that the runtime implementation provided by sandbox runtime provisioner
		// implements the RuntimeEventEmitter interface.
		eventEmitter: rt.(host.RuntimeEventEmitter),
	}

	targetInfo, err := ts.init(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("error while initializing TEE state: %w", err)
	}
	if err = s.updateTargetInfo(ctx, targetInfo, conn); err != nil {
		return nil, fmt.Errorf("error while updating TEE target info: %w", err)
	}

	return &ts, nil
}

func (s *sgxProvisioner) updateTargetInfo(ctx context.Context, targetInfo []byte, conn protocol.Connection) error {
	_, err := conn.Call(
		ctx,
		&protocol.Body{
			RuntimeCapabilityTEERakInitRequest: &protocol.RuntimeCapabilityTEERakInitRequest{
				TargetInfo: targetInfo,
			},
		},
	)
	return err
}

func (s *sgxProvisioner) updateCapabilityTEE(ctx context.Context, ts *teeState, conn protocol.Connection) (*node.CapabilityTEE, error) {
	ctx, cancel := context.WithTimeout(ctx, runtimeRAKTimeout)
	defer cancel()

	// Update report target info in case the QE identity has changed (e.g. aesmd upgrade).
	targetInfo, err := ts.updateTargetInfo(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("error while updating TEE target info: %w", err)
	}
	if err = s.updateTargetInfo(ctx, targetInfo, conn); err != nil {
		return nil, fmt.Errorf("error while updating TEE target info: %w", err)
	}

	rakQuoteRes, err := conn.Call(
		ctx,
		&protocol.Body{
			RuntimeCapabilityTEERakReportRequest: &protocol.Empty{},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error while requesting worker quote and public RAK: %w", err)
	}
	rakPub := rakQuoteRes.RuntimeCapabilityTEERakReportResponse.RakPub
	rekPub := rakQuoteRes.RuntimeCapabilityTEERakReportResponse.RekPub
	report := rakQuoteRes.RuntimeCapabilityTEERakReportResponse.Report
	nonce := rakQuoteRes.RuntimeCapabilityTEERakReportResponse.Nonce

	attestation, err := ts.update(ctx, s, conn, report, nonce)
	if err != nil {
		return nil, err
	}

	capabilityTEE := &node.CapabilityTEE{
		Hardware:    node.TEEHardwareIntelSGX,
		RAK:         rakPub,
		REK:         rekPub,
		Attestation: attestation,
	}

	return capabilityTEE, nil
}

func (s *sgxProvisioner) attestationWorker(ts *teeState, hp *sandbox.HostInitializerParams) {
	t := time.NewTicker(s.cfg.RuntimeAttestInterval)
	defer t.Stop()

	logger := s.logger.With("runtime_id", ts.runtimeID)

	for {
		select {
		case <-hp.Process.Wait():
			// Process has terminated.
			return
		case <-t.C:
			// Re-attest based on the configured interval.
		case <-hp.NotifyUpdateCapabilityTEE:
			// Re-attest when explicitly requested. Also reset the periodic ticker to make sure we
			// don't needlessly re-attest too often.
			t.Reset(s.cfg.RuntimeAttestInterval)
		}

		// Update CapabilityTEE.
		logger.Info("regenerating CapabilityTEE")

		capabilityTEE, err := s.updateCapabilityTEE(context.Background(), ts, hp.Connection)
		if err != nil {
			logger.Error("failed to regenerate CapabilityTEE",
				"err", err,
			)
			continue
		}

		// Emit event about the updated CapabilityTEE.
		ts.eventEmitter.EmitEvent(&host.Event{Updated: &host.UpdatedEvent{
			Version:       hp.Version,
			CapabilityTEE: capabilityTEE,
		}})
	}
}

func (s *sgxProvisioner) getPlatformManifest() (platformManifest []byte, err error) {
	err = s.serviceStore.GetCBOR(platformManifestKey, &platformManifest)
	return
}

// Implements host.Provisioner.
func (s *sgxProvisioner) NewRuntime(cfg host.Config) (host.Runtime, error) {
	// Make sure to return an error early if the SGX runtime loader is not configured.
	if s.cfg.LoaderPath == "" {
		return nil, fmt.Errorf("SGX loader binary path is not configured")
	}

	return s.sandbox.NewRuntime(cfg)
}

// Implements host.Provisioner.
func (s *sgxProvisioner) Name() string {
	return "sgx"
}

// New creates a new Intel SGX runtime provisioner.
func New(cfg Config) (host.Provisioner, error) {
	// Use a default RuntimeAttestInterval if none was provided.
	if cfg.RuntimeAttestInterval == 0 {
		cfg.RuntimeAttestInterval = defaultRuntimeAttestInterval
	}

	initMetrics()

	s := &sgxProvisioner{
		cfg:          cfg,
		ias:          cfg.IAS,
		pcs:          cfg.PCS,
		aesm:         aesm.NewClient(aesmdSocketPath),
		consensus:    cfg.Consensus,
		logger:       logging.GetLogger("runtime/host/sgx"),
		serviceStore: cfg.CommonStore.GetServiceStore(serviceStoreName),
	}
	p, err := sandbox.New(sandbox.Config{
		GetSandboxConfig:  s.getSandboxConfig,
		HostInfo:          cfg.HostInfo,
		HostInitializer:   s.hostInitializer,
		InsecureNoSandbox: cfg.InsecureNoSandbox,
		Logger:            s.logger,
	})
	if err != nil {
		return nil, err
	}
	s.sandbox = p

	return s, nil
}
