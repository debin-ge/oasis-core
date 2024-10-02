// Package bundle implements support for unified runtime bundles.
package bundle

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/sgx"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/sigstruct"
	cmdFlags "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	"github.com/oasisprotocol/oasis-core/go/runtime/bundle/component"
)

// Bundle is a runtime bundle instance.
type Bundle struct {
	Manifest *Manifest
	Data     map[string]Data

	// archive is the underlying ZIP archive.
	archive *zip.ReadCloser
	// manifestHash is the original manifest hash of the bundle at time the bundle was loaded.
	manifestHash hash.Hash
}

// Validate validates the runtime bundle for well-formedness.
func (bnd *Bundle) Validate() error {
	// Ensure the manifest is valid.
	if err := bnd.Manifest.Validate(); err != nil {
		return fmt.Errorf("runtime/bundle: malformed manifest: %w", err)
	}

	// Ensure all the files in the manifest are present.
	type bundleFile struct {
		descr, fn string
		optional  bool
	}
	var needFiles []bundleFile
	for id, comp := range bnd.Manifest.GetAvailableComponents() {
		needFiles = append(needFiles, bundleFile{
			descr:    fmt.Sprintf("%s: ELF executable", id),
			fn:       comp.Executable,
			optional: true,
		})
		if sgx := comp.SGX; sgx != nil {
			needFiles = append(needFiles,
				[]bundleFile{
					{
						descr: fmt.Sprintf("%s: SGX executable", id),
						fn:    sgx.Executable,
					},
					{
						descr:    fmt.Sprintf("%s: SGX signature", id),
						fn:       sgx.Signature,
						optional: true,
					},
				}...,
			)
		}
		if tdx := comp.TDX; tdx != nil {
			needFiles = append(needFiles,
				[]bundleFile{
					{
						descr: fmt.Sprintf("%s: TDX virtual firmware", id),
						fn:    tdx.Firmware,
					},
					{
						descr:    fmt.Sprintf("%s: TDX kernel image", id),
						fn:       tdx.Kernel,
						optional: true,
					},
					{
						descr:    fmt.Sprintf("%s: TDX initrd image", id),
						fn:       tdx.InitRD,
						optional: true,
					},
					{
						descr:    fmt.Sprintf("%s: TDX VM stage 2 image", id),
						fn:       tdx.Stage2Image,
						optional: true,
					},
				}...,
			)
		}
	}
	for _, v := range needFiles {
		if v.fn == "" {
			if v.optional {
				continue
			}
			return fmt.Errorf("runtime/bundle: missing %s in manifest", v.descr)
		}
		if _, ok := bnd.Data[v.fn]; !ok {
			return fmt.Errorf("runtime/bundle: missing %s in bundle", v.descr)
		}
	}

	// Ensure all files in the bundle have a digest entry, and that the
	// extracted file's digest matches the one in the manifest.
	for fn, d := range bnd.Data {
		h, err := HashAllData(d)
		if err != nil {
			return fmt.Errorf("runtime/bundle: failed to read '%s': %w", fn, err)
		}

		mh, ok := bnd.Manifest.Digests[fn]
		if !ok {
			// Ignore the manifest not having a digest entry, though
			// it having one and being valid (while quite a feat) is
			// also ok.
			if fn == manifestName {
				continue
			}
			return fmt.Errorf("runtime/bundle: missing digest: '%s'", fn)
		}
		if !h.Equal(&mh) {
			return fmt.Errorf("runtime/bundle: invalid digest: '%s'", fn)
		}
	}

	for _, comp := range bnd.Manifest.GetAvailableComponents() {
		// Make sure the SGX signature is valid if it exists.
		if err := bnd.verifySgxSignature(comp); err != nil {
			return err
		}
	}

	return nil
}

// Add adds/overwrites a file to/in the bundle.
func (bnd *Bundle) Add(fn string, data Data) error {
	if filepath.Dir(fn) != "." {
		return fmt.Errorf("runtime/bundle: invalid filename for add: '%s'", fn)
	}

	if bnd.Manifest.Digests == nil {
		bnd.Manifest.Digests = make(map[string]hash.Hash)
	}
	if bnd.Data == nil {
		bnd.Data = make(map[string]Data)
	}

	h, err := HashAllData(data)
	if err != nil {
		return fmt.Errorf("runtime/bundle: failed to hash data: %w", err)
	}

	bnd.Manifest.Digests[fn] = h
	bnd.Data[fn] = data
	return nil
}

// MrEnclave returns the MRENCLAVE of the SGX excutable.
func (bnd *Bundle) MrEnclave(id component.ID) (*sgx.MrEnclave, error) {
	comp := bnd.Manifest.GetComponentByID(id)
	if comp == nil {
		return nil, fmt.Errorf("runtime/bundle: component '%s' not available", id)
	}
	if comp.SGX == nil {
		return nil, fmt.Errorf("runtime/bundle: no SGX metadata for '%s'", id)
	}
	d, ok := bnd.Data[comp.SGX.Executable]
	if !ok {
		return nil, fmt.Errorf("runtime/bundle: no SGX executable for '%s'", id)
	}
	f, err := d.Open()
	if err != nil {
		return nil, fmt.Errorf("runtime/bundle: failed to open SGX executable for '%s': %w", id, err)
	}
	defer f.Close()

	var mrEnclave sgx.MrEnclave
	if err := mrEnclave.FromSgxs(f); err != nil {
		return nil, fmt.Errorf("runtime/bundle: failed to derive SGX MRENCLAVE for '%s': %w", id, err)
	}
	return &mrEnclave, nil
}

// MrSigner returns the MRSIGNER that signed the SGX executable.
func (bnd *Bundle) MrSigner(id component.ID) (*sgx.MrSigner, error) {
	comp := bnd.Manifest.GetComponentByID(id)
	if comp == nil {
		return nil, fmt.Errorf("runtime/bundle: component '%s' not available", id)
	}
	if comp.SGX == nil {
		return nil, fmt.Errorf("runtime/bundle: no SGX metadata for '%s'", id)
	}

	var mrSigner sgx.MrSigner
	switch {
	case comp.SGX.Signature == "" && cmdFlags.DebugDontBlameOasis():
		// Use dummy signer (only in tests).
		mrSigner = sgx.FortanixDummyMrSigner
	default:
		// Load the actual signature.
		d, ok := bnd.Data[comp.SGX.Signature]
		if !ok {
			return nil, fmt.Errorf("runtime/bundle: no SGX signature for '%s'", id)
		}
		b, err := ReadAllData(d)
		if err != nil {
			return nil, fmt.Errorf("runtime/bundle: failed to read SGX signature for '%s': %w", id, err)
		}

		sigPk, _, err := sigstruct.Verify(b)
		if err != nil {
			return nil, err
		}
		if err = mrSigner.FromPublicKey(sigPk); err != nil {
			return nil, err
		}
	}
	return &mrSigner, nil
}

// EnclaveIdentity returns the SGX enclave identity of the given component.
func (bnd *Bundle) EnclaveIdentity(id component.ID) (*sgx.EnclaveIdentity, error) {
	mrEnclave, err := bnd.MrEnclave(id)
	if err != nil {
		return nil, err
	}

	mrSigner, err := bnd.MrSigner(id)
	if err != nil {
		return nil, err
	}

	return &sgx.EnclaveIdentity{
		MrEnclave: *mrEnclave,
		MrSigner:  *mrSigner,
	}, nil
}

func (bnd *Bundle) verifySgxSignature(comp *Component) error {
	if comp.SGX == nil || comp.SGX.Signature == "" {
		return nil
	}

	mrEnclave, err := bnd.MrEnclave(comp.ID())
	if err != nil {
		return err
	}

	d, ok := bnd.Data[comp.SGX.Signature]
	if !ok {
		return fmt.Errorf("runtime/bundle: no SGX signature for '%s'", comp.ID())
	}
	b, err := ReadAllData(d)
	if err != nil {
		return fmt.Errorf("runtime/bundle: failed to read SGX signature for '%s': %w", comp.ID(), err)
	}
	_, sigStruct, err := sigstruct.Verify(b)
	if err != nil {
		return fmt.Errorf("runtime/bundle: failed to verify sigstruct for '%s': %w", comp.ID(), err)
	}

	if sigStruct.EnclaveHash != *mrEnclave {
		return fmt.Errorf("runtime/bundle: sigstruct for '%s' does not match SGXS (got: %s expected: %s)", comp.ID(), sigStruct.EnclaveHash, *mrEnclave)
	}

	return nil
}

// ResetManifest removes the serialized manifest from the bundle so that it can be regenerated on
// the next call to Write.
//
// This needs to be used after doing modifications to bundles.
func (bnd *Bundle) ResetManifest() {
	delete(bnd.Data, manifestName)
}

// Write serializes a runtime bundle to the on-disk representation.
func (bnd *Bundle) Write(fn string) error {
	// Ensure the bundle is well-formed.
	if err := bnd.Validate(); err != nil {
		return fmt.Errorf("runtime/bundle: refusing to write malformed bundle: %w", err)
	}

	// Serialize the manifest.
	rawManifest, err := json.Marshal(bnd.Manifest)
	if err != nil {
		return fmt.Errorf("runtime/bundle: failed to serialize manifest: %w", err)
	}
	if bnd.Data[manifestName] != nil {
		// While this is "ok", instead of trying to figure out if the
		// deserialized manifest matches the serialied one, just bail.
		return fmt.Errorf("runtime/bundle: data contains manifest entry")
	}

	// Write out the archive to a in-memory buffer, taking care to ensure
	// that the manifest is the 0th entry.
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	type writeFile struct {
		fn string
		d  Data
	}
	writeFiles := []writeFile{
		{
			fn: manifestName,
			d:  NewBytesData(rawManifest),
		},
	}
	for f := range bnd.Data {
		writeFiles = append(writeFiles, writeFile{
			fn: f,
			d:  bnd.Data[f],
		})
	}
	for _, f := range writeFiles {
		sf, wErr := f.d.Open()
		if wErr != nil {
			return fmt.Errorf("runtime/bundle: failed to open data for '%s': %w", f.fn, wErr)
		}

		fw, wErr := w.Create(f.fn)
		if wErr != nil {
			_ = sf.Close()
			return fmt.Errorf("runtime/bundle: failed to create file '%s': %w", f.fn, wErr)
		}

		if _, wErr = io.Copy(fw, sf); wErr != nil {
			_ = sf.Close()
			return fmt.Errorf("runtime/bundle: failed to write file '%s': %w", f.fn, wErr)
		}
		_ = sf.Close()
	}
	if err = w.Close(); err != nil {
		return fmt.Errorf("runtime/bundle: failed to finalize bundle: %w", err)
	}

	if err = os.WriteFile(fn, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("runtime/bundle: failed to write bundle: %w", err)
	}

	// Update the manifest hash.
	bnd.manifestHash = bnd.Manifest.Hash()

	return nil
}

// ExplodedPath returns the path under the data directory that contains all of the exploded bundles.
func ExplodedPath(dataDir string) string {
	return filepath.Join(dataDir, "runtimes", "bundles")
}

// DetachedExplodedPath returns the path under the data directory that contains all of the detached
// exploded bundles.
func DetachedExplodedPath(dataDir string) string {
	return filepath.Join(ExplodedPath(dataDir), "detached")
}

// ExplodedPath returns the path that the corresponding asset will be written to via WriteExploded.
func (bnd *Bundle) ExplodedPath(dataDir, fn string) string {
	var subDir string
	switch bnd.Manifest.IsDetached() {
	case false:
		// DATADIR/runtimes/bundles/manifestHash
		subDir = filepath.Join(ExplodedPath(dataDir),
			bnd.manifestHash.String(),
		)
	case true:
		// DATADIR/runtimes/bundles/detached/manifestHash
		subDir = filepath.Join(DetachedExplodedPath(dataDir),
			bnd.manifestHash.String(),
		)
	}

	if fn == "" {
		return subDir
	}
	return filepath.Join(subDir, fn)
}

// WriteExploded writes the extracted runtime bundle to the appropriate
// location under the specified data directory.
func (bnd *Bundle) WriteExploded(dataDir string) error {
	if err := bnd.Validate(); err != nil {
		return fmt.Errorf("runtime/bundle: refusing to explode malformed bundle: %w", err)
	}

	subDir := bnd.ExplodedPath(dataDir, "")

	// Check to see if we have done this before, and be nice to SSDs by
	// just verifying extracted data for correctness.
	switch _, err := os.Stat(subDir); err {
	case nil:
		// Validate that the on-disk assets match the bundle contents.
		//
		// Note: This ignores extra garbage that may be on disk, but
		// people that mess with internal directories get what they
		// deserve.
		for fn, expected := range bnd.Data {
			fn = bnd.ExplodedPath(dataDir, fn)
			f, rdErr := os.Open(fn)
			if rdErr != nil {
				return fmt.Errorf("runtime/bundle: failed to re-load asset '%s': %w", fn, rdErr)
			}
			h, rdErr := hash.NewFromReader(f)
			if rdErr != nil {
				_ = f.Close()
				return fmt.Errorf("runtime/bundle: failed to re-load asset '%s': %w", fn, rdErr)
			}
			_ = f.Close()

			he, rdErr := HashAllData(expected)
			if rdErr != nil {
				return fmt.Errorf("runtime/bundle: failed to re-load asset '%s': %w", fn, rdErr)
			}

			if !h.Equal(&he) {
				return fmt.Errorf("runtime/bundle: corrupt asset: '%s'", fn)
			}
		}
	default:
		// Extract the bundle to disk.
		if !os.IsNotExist(err) {
			return fmt.Errorf("runtime/bundle: failed to stat asset directory '%s': %w", subDir, err)
		}

		for _, v := range []string{
			subDir,
			bnd.ExplodedPath(dataDir, manifestPath),
		} {
			if err = os.MkdirAll(v, 0o700); err != nil {
				return fmt.Errorf("runtime/bundle: failed to create asset sub-dir '%s': %w", v, err)
			}
		}
		for fn, data := range bnd.Data {
			fn = bnd.ExplodedPath(dataDir, fn)

			var src io.ReadCloser
			if src, err = data.Open(); err != nil {
				return fmt.Errorf("runtime/bundle: failed to open source asset '%s': %w", fn, err)
			}

			var f *os.File
			if f, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600); err != nil {
				_ = src.Close()
				return fmt.Errorf("runtime/bundle: failed to write asset '%s': %w", fn, err)
			}
			if _, err = io.Copy(f, src); err != nil {
				_ = src.Close()
				_ = f.Close()
				return fmt.Errorf("runtime/bundle: failed to write asset '%s': %w", fn, err)
			}
			_ = src.Close()
			_ = f.Close()
		}

		for id, comp := range bnd.Manifest.GetAvailableComponents() {
			if comp.Executable != "" {
				if err := os.Chmod(bnd.ExplodedPath(dataDir, comp.Executable), 0o700); err != nil {
					return fmt.Errorf("runtime/bundle: failed to fixup executable permissions for '%s': %w", id, err)
				}
			}
		}
	}

	return nil
}

// Close closes the bundle, releasing resources.
func (bnd *Bundle) Close() error {
	bnd.Manifest = nil
	bnd.Data = nil
	if bnd.archive != nil {
		bnd.archive.Close()
		bnd.archive = nil
	}
	bnd.manifestHash.Empty()
	return nil
}

// Open opens and validates a runtime bundle instance.
func Open(fn string) (*Bundle, error) {
	r, err := zip.OpenReader(fn)
	if err != nil {
		return nil, fmt.Errorf("runtime/bundle: failed to open bundle: %w", err)
	}

	// Read the contents.
	data := make(map[string]Data)
	for i, v := range r.File {
		// Sanitize the file name by ensuring that all names are rooted
		// at the correct location.
		switch i {
		case 0:
			// Much like the JAR files, the manifest MUST come first.
			if v.Name != manifestName {
				return nil, fmt.Errorf("runtime/bundle: invalid manifest file name: '%s'", v.Name)
			}
		default:
			if filepath.Dir(v.Name) != "." {
				return nil, fmt.Errorf("runtime/bundle: failed to sanitize path '%s'", v.Name)
			}
		}

		data[v.Name] = v
	}

	// Decode the manifest.
	var manifest Manifest
	d, ok := data[manifestName]
	if !ok {
		return nil, fmt.Errorf("runtime/bundle: missing manifest")
	}
	b, err := ReadAllData(d)
	if err != nil {
		return nil, fmt.Errorf("runtime/bundle: failed to read manifest: %w", err)
	}
	if err = json.Unmarshal(b, &manifest); err != nil {
		return nil, fmt.Errorf("runtime/bundle: failed to parse manifest: %w", err)
	}

	// Ensure the bundle is well-formed.
	bnd := &Bundle{
		Manifest:     &manifest,
		Data:         data,
		archive:      r,
		manifestHash: manifest.Hash(),
	}
	if err = bnd.Validate(); err != nil {
		return nil, err
	}

	return bnd, nil
}

// Data is a data item in the bundle.
type Data interface {
	// Open returns an io.ReadCloser that can be used to access the underlying data.
	Open() (io.ReadCloser, error)
}

// bytesData is an internal wrapper for using raw bytes as data.
type bytesData []byte

func (b bytesData) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b)), nil
}

// NewBytesData creates a new Data instance from the given byte slice. The slice is not copied.
func NewBytesData(b []byte) Data {
	return bytesData(b)
}

// fileData is an internal wrapper for using a file path as data.
type fileData string

func (f fileData) Open() (io.ReadCloser, error) {
	return os.Open(string(f))
}

// NewFileData creates a new Data instance that opens and reads the given file path.
func NewFileData(fn string) Data {
	return fileData(fn)
}

// ReadAllData reads all of the underlying data into a buffer and returns it.
func ReadAllData(d Data) ([]byte, error) {
	f, err := d.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// HashAllData hashes all of the underlying data and returns the hash.
func HashAllData(d Data) (hash.Hash, error) {
	f, err := d.Open()
	if err != nil {
		return hash.Hash{}, err
	}
	defer f.Close()
	return hash.NewFromReader(f)
}
