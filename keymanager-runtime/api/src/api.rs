use failure::Fail;
use rand::{rngs::OsRng, Rng};
use serde_derive::{Deserialize, Serialize};
use x25519_dalek;

use ekiden_runtime::{
    common::{crypto::signature::Signature, runtime::RuntimeId},
    impl_bytes, runtime_api,
};

impl_bytes!(ContractId, 32, "A 256-bit contract identifier.");
impl_bytes!(PrivateKey, 32, "A private key.");
impl_bytes!(PublicKey, 32, "A public key.");
impl_bytes!(StateKey, 32, "A state key.");
impl_bytes!(MasterSecret, 32, "A 256 bit master secret.");

/// Key manager initialization request.
#[derive(Clone, Serialize, Deserialize)]
pub struct InitRequest {
    // TODO: Policy, peers, checksum, etc.
}

/// Key manager initialization response.
#[derive(Clone, Serialize, Deserialize)]
pub struct InitResponse {
    /// True iff the key manager thinks it's running in a secure mode.
    pub is_secure: bool,
    /// Checksum for validating replication.
    #[serde(with = "serde_bytes")]
    pub checksum: Vec<u8>,
}

/// Request runtime/contract id tuple.
#[derive(Clone, Serialize, Deserialize)]
pub struct RequestIds {
    /// Runtime ID.
    pub runtime_id: RuntimeId,
    /// Contract ID.
    pub contract_id: ContractId,
}

impl RequestIds {
    pub fn new(runtime_id: RuntimeId, contract_id: ContractId) -> Self {
        Self {
            runtime_id,
            contract_id,
        }
    }

    pub fn to_cache_key(&self) -> Vec<u8> {
        let mut k = self.runtime_id.as_ref().to_vec();
        k.extend_from_slice(self.contract_id.as_ref());
        k
    }
}

/// Keys for a contract.
#[derive(Clone, Serialize, Deserialize)]
pub struct ContractKey {
    /// Input key pair (pk, sk)
    pub input_keypair: InputKeyPair,
    /// State encryption key
    pub state_key: StateKey,
}

impl ContractKey {
    /// Generate a new random key (for testing).
    pub fn generate_mock() -> Self {
        let mut rng = OsRng::new().unwrap();
        let sk = x25519_dalek::StaticSecret::new(&mut rng);
        let pk = x25519_dalek::PublicKey::from(&sk);

        let mut state_key = StateKey::default();
        rng.fill(&mut state_key.0);

        ContractKey::new(
            PublicKey(*pk.as_bytes()),
            PrivateKey(sk.to_bytes()),
            state_key,
        )
    }

    /// Create a set of `ContractKey`.
    pub fn new(pk: PublicKey, sk: PrivateKey, k: StateKey) -> Self {
        Self {
            input_keypair: InputKeyPair { pk, sk },
            state_key: k,
        }
    }

    /// Create a set of `ContractKey` with only the public key.
    pub fn from_public_key(k: PublicKey) -> Self {
        Self {
            input_keypair: InputKeyPair {
                pk: k,
                sk: PrivateKey::default(),
            },
            state_key: StateKey::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InputKeyPair {
    /// Pk
    pk: PublicKey,
    /// sk
    sk: PrivateKey,
}

impl InputKeyPair {
    pub fn new(pk: PublicKey, sk: PrivateKey) -> Self {
        Self { pk, sk }
    }

    pub fn get_pk(&self) -> PublicKey {
        self.pk
    }

    pub fn get_sk(&self) -> PrivateKey {
        self.sk
    }
}

/// Context used for the public key signature.
pub const PUBLIC_KEY_CONTEXT: [u8; 8] = *b"EkKmPubK";

/// Signed public key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedPublicKey {
    /// Public key.
    pub key: PublicKey,
    /// Timestamp representing the expiry of the returned key.
    pub timestamp: Option<u64>,
    /// Sign(sk, (key || timestamp)) from the key manager.
    pub signature: Signature,
}

/// Key manager error.
#[derive(Debug, Fail)]
pub enum KeyManagerError {
    #[fail(display = "client session is not authenticated")]
    NotAuthenticated,
    #[fail(display = "key manager is not initialized")]
    NotInitialized,
    #[fail(display = "key manager is already initialized")]
    AlreadyInitialized,
}

runtime_api! {
    pub fn get_or_create_keys(RequestIds) -> ContractKey;

    pub fn get_public_key(RequestIds) -> Option<SignedPublicKey>;

    pub fn get_long_term_public_key(RequestIds) -> Option<SignedPublicKey>;
}
