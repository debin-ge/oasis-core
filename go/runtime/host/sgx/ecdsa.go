package sgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/aesm"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/pcs"
	sgxQuote "github.com/oasisprotocol/oasis-core/go/common/sgx/quote"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/host/protocol"
)

type teeStateECDSA struct {
	teeStateImplCommon

	key *aesm.AttestationKeyID

	tcbCache *tcbCache
}

func (ec *teeStateECDSA) Init(ctx context.Context, sp *sgxProvisioner, runtimeID common.Namespace, version version.Version) ([]byte, error) {
	// Check whether the consensus layer even supports ECDSA attestations.
	regParams, err := sp.consensus.Registry().ConsensusParameters(ctx, consensus.HeightLatest)
	if err != nil {
		return nil, fmt.Errorf("unable to determine registry consensus parameters: %w", err)
	}
	if regParams.TEEFeatures == nil || !regParams.TEEFeatures.SGX.PCS {
		return nil, fmt.Errorf("ECDSA not supported by the registry")
	}

	// Fetch supported attestation keys.
	akeys, err := sp.aesm.GetAttestationKeyIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch attestation key IDs: %w", err)
	}

	// Find the first suitable ECDSA-capable key.
	var key *aesm.AttestationKeyID
	for _, akey := range akeys {
		if akey.Type == aesm.AttestationKeyECDSA_P256 {
			key = akey
			break
		}
	}
	if key == nil {
		return nil, fmt.Errorf("no suitable ECDSA attestation keys found")
	}

	// Retrieve the target info for QE.
	targetInfo, err := sp.aesm.GetTargetInfo(ctx, key)
	if err != nil {
		return nil, err
	}

	ec.runtimeID = runtimeID
	ec.version = version
	ec.key = key

	ec.tcbCache = newTcbCache(sp.serviceStore, sp.logger)

	return targetInfo, nil
}

func (ec *teeStateECDSA) verifyBundle(quote pcs.Quote, quotePolicy *pcs.QuotePolicy, tcbBundle *pcs.TCBBundle, sp *sgxProvisioner, which string) error {
	if tcbBundle == nil {
		return fmt.Errorf("nil bundle is not valid")
	}
	_, err := quote.Verify(quotePolicy, time.Now(), tcbBundle)
	var tcbErr *pcs.TCBOutOfDateError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &tcbErr):
		sp.logger.Error("TCB is not up to date",
			"which", which,
			"kind", tcbErr.Kind,
			"tcb_status", tcbErr.Status.String(),
			"advisory_ids", tcbErr.AdvisoryIDs,
		)
		return tcbErr
	default:
		return fmt.Errorf("quote verification failed (%s bundle): %w", which, err)
	}
}

func (ec *teeStateECDSA) Update(ctx context.Context, sp *sgxProvisioner, conn protocol.Connection, report []byte, _ string) ([]byte, error) {
	rawQuote, err := sp.aesm.GetQuoteEx(ctx, ec.key, report)
	if err != nil {
		return nil, fmt.Errorf("failed to get quote: %w", err)
	}

	var quote pcs.Quote
	if err = quote.UnmarshalBinary(rawQuote); err != nil {
		return nil, fmt.Errorf("failed to parse quote: %w", err)
	}

	// Check what information we need to retrieve based on what is in the quote.
	qs, ok := quote.Signature.(*pcs.QuoteSignatureECDSA_P256)
	if !ok {
		return nil, fmt.Errorf("unsupported attestation key type: %s", quote.Signature.AttestationKeyType())
	}

	switch qs.CertificationData.(type) {
	case *pcs.CertificationData_PCKCertificateChain:
		// We have a PCK certificate chain and so are good to go.
	case *pcs.CertificationData_PPID:
		// We have a PPID, need to retrieve PCK certificate first.
		// TODO: Fetch PCK certificate based on PPID and include it in the quote, replacing the
		//       PPID certification data with the PCK certificate chain certification data.
		//       e.g. sp.pcs.GetPCKCertificateChain(ctx, nil, data.PPID, data.CPUSVN, data.PCESVN, data.PCEID)
		//
		//	 Due to aesmd QuoteEx APIs not supporting certification data this currently
		//       cannot be easily implemented. Instead we rely on a quote provider to be installed.
		return nil, fmt.Errorf("PPID certification data not yet supported; please install a quote provider")
	default:
		return nil, fmt.Errorf("unsupported certification data type: %s", qs.CertificationData.CertificationDataType())
	}

	// Verify PCK certificate and extract the information required to get the TCB bundle.
	pckInfo, err := qs.VerifyPCK(time.Now())
	if err != nil {
		return nil, fmt.Errorf("PCK verification failed: %w", err)
	}

	// Get current quote policy from the consensus layer.
	var quotePolicy *pcs.QuotePolicy
	var policies *sgxQuote.Policy
	policies, err = ec.getQuotePolicies(ctx, sp)
	if err != nil {
		return nil, err
	}
	if policies != nil {
		quotePolicy = policies.PCS
	}

	// Verify the quote so we can catch errors early (the runtime and later consensus layer will
	// also do their own verification).
	// Check bundles in order: fresh first, then cached, then try downloading again if there was
	// no scheduled refresh this time.
	tcbBundle, err := func() (*pcs.TCBBundle, error) {
		var fresh *pcs.TCBBundle

		cached, refresh := ec.tcbCache.check(pckInfo.FMSPC)
		if refresh {
			if fresh, err = sp.pcs.GetTCBBundle(ctx, pckInfo.FMSPC); err != nil {
				sp.logger.Warn("error downloading TCB refresh",
					"err", err,
				)
			}
			if err = ec.verifyBundle(quote, quotePolicy, fresh, sp, "fresh"); err == nil {
				ec.tcbCache.cache(fresh, pckInfo.FMSPC)
				return fresh, nil
			}
			sp.logger.Warn("error verifying downloaded TCB refresh",
				"err", err,
			)
		}

		if err = ec.verifyBundle(quote, quotePolicy, cached, sp, "cached"); err == nil {
			return cached, nil
		}

		// If downloaded already, don't try again but just return the last error.
		if refresh {
			return nil, fmt.Errorf("both fresh and cached TCB bundles failed verification, cached error: %w", err)
		}

		// If not downloaded yet this time round, try forcing. Any errors are fatal.
		if fresh, err = sp.pcs.GetTCBBundle(ctx, pckInfo.FMSPC); err != nil {
			sp.logger.Warn("error downloading TCB",
				"err", err,
			)
			return nil, err
		}
		if err = ec.verifyBundle(quote, quotePolicy, fresh, sp, "downloaded"); err != nil {
			return nil, err
		}
		ec.tcbCache.cache(fresh, pckInfo.FMSPC)
		return fresh, nil
	}()
	if err != nil {
		return nil, err
	}

	// Prepare quote structure.
	q := sgxQuote.Quote{
		PCS: &pcs.QuoteBundle{
			Quote: rawQuote,
			TCB:   *tcbBundle,
		},
	}

	// Call the runtime with the quote and TCB bundle.
	rspBody, err := conn.Call(
		ctx,
		&protocol.Body{
			RuntimeCapabilityTEERakQuoteRequest: &protocol.RuntimeCapabilityTEERakQuoteRequest{
				Quote: q,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error while configuring quote: %w", err)
	}
	rsp := rspBody.RuntimeCapabilityTEERakQuoteResponse
	if rsp == nil {
		return nil, fmt.Errorf("unexpected response from runtime")
	}

	return cbor.Marshal(node.SGXAttestation{
		Versioned: cbor.NewVersioned(node.LatestSGXAttestationVersion),
		Quote:     q,
		Height:    rsp.Height,
		Signature: rsp.Signature,
	}), nil
}
