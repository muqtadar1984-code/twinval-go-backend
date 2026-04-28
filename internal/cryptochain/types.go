// Package cryptochain implements Patent Module 350 — the crypto-linked
// valuation module.
//
// Every RTPMV computation produces a ValuationToken that is appended to
// an immutable, SHA-256-linked chain. Each token carries the hash of the
// previous token's TokenHash in its PrecedingHash field, so any retroactive
// edit to a past token breaks the chain at the edited index and at every
// downstream link.
//
// The package also supports an audit-mode export (ToAuditMode) that
// surfaces the frozen calibration parameters alongside each token, so
// independent auditors can reconstruct valuations without raw sensor data
// (patent paragraph [0057]).
//
// VALIDATION NOTE:
// Hash inputs (the byte sequence fed to SHA-256) must be byte-identical
// to the Python POC. All float fields are serialised with %.10f to match
// Python's repr-equivalent fixed-point format. Field order matches the
// declaration order of the originating struct.
package cryptochain

// ZeroHash is the PrecedingHash carried by the genesis token. Exactly
// 64 lowercase '0' characters — the hex representation of an all-zero
// 32-byte SHA-256 placeholder.
const ZeroHash = "0000000000000000000000000000000000000000000000000000000000000000"

// ValuationToken is one record in the chain. Once Append() returns, the
// token is immutable for normal use — the chain stores values, not pointers.
//
// Field declaration order is load-bearing: HashToken serialises fields in
// this order, and any reordering breaks parity with the Python POC.
type ValuationToken struct {
	TokenID             uint64
	PropertyID          string
	Timestamp           int64   // Unix nanoseconds
	ConditionedDataHash string  // hex SHA-256 of marshalled ConditionedData
	IndicatorsHash      string  // hex SHA-256 of marshalled TechnicalIndicators
	RTPMV               float64 // Real-Time Property Market Value
	Currency            string  // ISO 4217 (e.g., "MYR", "USD")
	PrecedingHash       string  // hex SHA-256 of previous TokenHash; ZeroHash on genesis
	TokenHash           string  // hex SHA-256 of all fields above in declaration order
}

// FrozenParameters captures the calibration constants that were active
// when a token was minted. Embedded in every AuditRecord so an auditor
// can reproduce the valuation arithmetic without sensor data access.
//
// The five indicator weights default to 1.0 because the Health Factor
// formula in compute is multiplicative — these are the implicit unit
// weights. Override per-instance if a weighted variant is in use.
type FrozenParameters struct {
	SHFWeight float64
	ESFWeight float64
	USSWeight float64
	PDPWeight float64
	CIWeight  float64

	FirstTradingThreshold  float64 // HF below this → Restricted state
	SecondTradingThreshold float64 // HF below this → Halted state

	LandValue      float64 // baseline component used in RTPMV
	StructureValue float64 // baseline component multiplied by HF in RTPMV
}

// AuditRecord is the externally-shareable audit-mode export. The token
// alone proves chain membership; the frozen parameters prove how the
// recorded RTPMV was derived.
type AuditRecord struct {
	Token            ValuationToken
	FrozenParameters FrozenParameters
}

// VerificationReport is the output of Verify.
type VerificationReport struct {
	// PerToken[i] is true when token i passes both its self-integrity
	// check (HashToken matches stored TokenHash) and its preceding-link
	// check (PrecedingHash matches the prior token's stored TokenHash).
	PerToken []bool

	// ChainValid is true when every token passes both checks.
	ChainValid bool

	// BrokenAtIndex is the lowest index that failed either check.
	// -1 when ChainValid is true.
	BrokenAtIndex int
}

// ChainConfig holds the FrozenParameters template stamped into every
// AuditRecord produced by ToAuditMode.
type ChainConfig struct {
	FrozenParameters FrozenParameters
}

// DefaultChainConfig returns a ChainConfig with the production-default
// frozen parameters. Trading thresholds default to 0.7 / 0.3 — the
// standard Active / Restricted / Halted band edges.
func DefaultChainConfig() ChainConfig {
	return ChainConfig{
		FrozenParameters: FrozenParameters{
			SHFWeight: 1.0,
			ESFWeight: 1.0,
			USSWeight: 1.0,
			PDPWeight: 1.0,
			CIWeight:  1.0,

			FirstTradingThreshold:  0.7,
			SecondTradingThreshold: 0.3,

			LandValue:      0.0,
			StructureValue: 0.0,
		},
	}
}
