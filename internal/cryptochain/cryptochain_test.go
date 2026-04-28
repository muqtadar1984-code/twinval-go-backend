package cryptochain

import (
	"strings"
	"testing"

	"github.com/twinval/internal/compute"
)

func sampleConditioned() compute.ConditionedData {
	return compute.ConditionedData{
		VibrationMagnitude:     0.05,
		StrainMagnitude:        100.0,
		Temperature:            22.0,
		Humidity:               50.0,
		AirQualityPM:           10.0,
		OccupancyRatio:         0.5,
		ElectricalLoad:         0.5,
		WaterConsumption:       0.5,
		UptimeContinuity:       1.0,
		CrossSensorConsistency: 1.0,
		CalibrationRecency:     30.0,
		TamperScore:            1.0,
		ChronologicalAge:       5.0,
		MaintenanceSensitivity: 0.5,
		ConditionQuality:       1.0,
	}
}

func sampleIndicators() compute.TechnicalIndicators {
	return compute.TechnicalIndicators{SHF: 1.0, ESF: 1.0, USS: 0.5, PDP: 0.9, CI: 1.0}
}

// sampleTokenInput returns a partial ValuationToken with the caller-supplied
// fields populated; chain-managed fields (TokenID, PrecedingHash, TokenHash)
// are filled in by Append.
func sampleTokenInput(rtpmv float64) ValuationToken {
	return ValuationToken{
		PropertyID:          "PROP-001",
		Timestamp:           1234567890,
		ConditionedDataHash: HashConditionedData(sampleConditioned()),
		IndicatorsHash:      HashIndicators(sampleIndicators()),
		RTPMV:               rtpmv,
		Currency:            "MYR",
	}
}

// =============================================================================
// Hash determinism
// =============================================================================

func TestHashConditionedData_Deterministic(t *testing.T) {
	h1 := HashConditionedData(sampleConditioned())
	h2 := HashConditionedData(sampleConditioned())
	if h1 != h2 {
		t.Errorf("nondeterministic hash: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("hash length: got %d, want 64", len(h1))
	}
}

func TestHashIndicators_Deterministic(t *testing.T) {
	h1 := HashIndicators(sampleIndicators())
	h2 := HashIndicators(sampleIndicators())
	if h1 != h2 {
		t.Errorf("nondeterministic hash: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("hash length: got %d, want 64", len(h1))
	}
}

func TestHashToken_Deterministic(t *testing.T) {
	tok := ValuationToken{
		TokenID:             1,
		PropertyID:          "PROP-001",
		Timestamp:           1234567890,
		ConditionedDataHash: strings.Repeat("a", 64),
		IndicatorsHash:      strings.Repeat("b", 64),
		RTPMV:               1_000_000.0,
		Currency:            "MYR",
		PrecedingHash:       ZeroHash,
	}
	h1 := HashToken(tok)
	h2 := HashToken(tok)
	if h1 != h2 {
		t.Errorf("nondeterministic hash: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("hash length: got %d, want 64", len(h1))
	}
}

// =============================================================================
// Genesis token
// =============================================================================

func TestGenesisToken_PrecedingHashIs64Zeros(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	tok, err := chain.Append(sampleTokenInput(1_000_000.0))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := strings.Repeat("0", 64)
	if tok.PrecedingHash != want {
		t.Errorf("PrecedingHash: got %q, want %q", tok.PrecedingHash, want)
	}
	if tok.TokenID != 1 {
		t.Errorf("TokenID: got %d, want 1", tok.TokenID)
	}
}

// =============================================================================
// Append + Length
// =============================================================================

func TestAppendTen_ChainLengthAndHeadAreCorrect(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	for i := 0; i < 10; i++ {
		if _, err := chain.Append(sampleTokenInput(float64(i * 100))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if chain.Len() != 10 {
		t.Errorf("Len: got %d, want 10", chain.Len())
	}
	head, ok := chain.Head()
	if !ok {
		t.Fatal("Head: ok=false on non-empty chain")
	}
	if head.TokenID != 10 {
		t.Errorf("Head TokenID: got %d, want 10", head.TokenID)
	}
}

func TestAppend_ZeroValueTokenReturnsError(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	_, err := chain.Append(ValuationToken{})
	if err == nil {
		t.Errorf("expected error appending zero-value token, got nil")
	}
}

// =============================================================================
// Verify
// =============================================================================

func TestVerify_EmptyChainIsValid(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	rep := Verify(chain)
	if !rep.ChainValid {
		t.Errorf("empty chain should be valid")
	}
	if rep.BrokenAtIndex != -1 {
		t.Errorf("BrokenAtIndex: got %d, want -1", rep.BrokenAtIndex)
	}
}

func TestVerify_UntamperedChainOfTenIsValid(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	for i := 0; i < 10; i++ {
		if _, err := chain.Append(sampleTokenInput(float64(i * 100))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	rep := Verify(chain)
	if !rep.ChainValid {
		t.Errorf("ChainValid: got false; per-token=%v; brokenAt=%d", rep.PerToken, rep.BrokenAtIndex)
	}
	if rep.BrokenAtIndex != -1 {
		t.Errorf("BrokenAtIndex: got %d, want -1", rep.BrokenAtIndex)
	}
	for i, ok := range rep.PerToken {
		if !ok {
			t.Errorf("token %d: per-token false on untampered chain", i)
		}
	}
}

func TestVerify_ModifiedRTPMVAtIndex5BreaksChain(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	for i := 0; i < 10; i++ {
		if _, err := chain.Append(sampleTokenInput(float64(i * 100))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Tamper: same package can read/write internal slice.
	chain.tokens[5].RTPMV = 999_999.99

	rep := Verify(chain)
	if rep.ChainValid {
		t.Errorf("ChainValid: got true after tamper at index 5")
	}
	if rep.BrokenAtIndex != 5 {
		t.Errorf("BrokenAtIndex: got %d, want 5", rep.BrokenAtIndex)
	}
	if rep.PerToken[5] {
		t.Errorf("PerToken[5]: got true, want false")
	}
	// Tampered token's downstream link is unaffected because tokens[5].TokenHash
	// (still the original) is what tokens[6].PrecedingHash points at.
	if !rep.PerToken[6] {
		t.Errorf("PerToken[6]: got false, want true (downstream link untouched)")
	}
}

func TestVerify_ModifiedPrecedingHashAtIndex7BreaksChain(t *testing.T) {
	chain := NewChain(DefaultChainConfig())
	for i := 0; i < 10; i++ {
		if _, err := chain.Append(sampleTokenInput(float64(i * 100))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	chain.tokens[7].PrecedingHash = strings.Repeat("a", 64)

	rep := Verify(chain)
	if rep.ChainValid {
		t.Errorf("ChainValid: got true after tamper at index 7")
	}
	if rep.BrokenAtIndex != 7 {
		t.Errorf("BrokenAtIndex: got %d, want 7", rep.BrokenAtIndex)
	}
	if rep.PerToken[7] {
		t.Errorf("PerToken[7]: got true, want false")
	}
}

// =============================================================================
// Audit mode
// =============================================================================

func TestToAuditMode_OneRecordPerToken(t *testing.T) {
	cfg := DefaultChainConfig()
	cfg.FrozenParameters.LandValue = 500_000
	cfg.FrozenParameters.StructureValue = 1_000_000
	chain := NewChain(cfg)
	for i := 0; i < 5; i++ {
		if _, err := chain.Append(sampleTokenInput(float64(i * 100))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	audit := chain.ToAuditMode()
	if len(audit) != 5 {
		t.Errorf("audit length: got %d, want 5", len(audit))
	}
	for i, rec := range audit {
		if rec.Token.TokenID != uint64(i+1) {
			t.Errorf("audit[%d].Token.TokenID: got %d, want %d", i, rec.Token.TokenID, i+1)
		}
		if rec.FrozenParameters.LandValue != 500_000 {
			t.Errorf("audit[%d].FrozenParameters.LandValue: got %v, want 500000", i, rec.FrozenParameters.LandValue)
		}
		if rec.FrozenParameters.FirstTradingThreshold != 0.7 {
			t.Errorf("audit[%d] FirstTradingThreshold: got %v, want 0.7", i, rec.FrozenParameters.FirstTradingThreshold)
		}
	}
}
