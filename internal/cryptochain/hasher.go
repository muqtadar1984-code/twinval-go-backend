package cryptochain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/twinval/internal/compute"
)

// =============================================================================
// Deterministic serialisation
//
// Each MarshalForHash function produces a single string with a fixed field
// separator ("|") and float fields formatted as "%.10f". This format must
// match the Python POC byte-for-byte for parity validation. Do not change
// the separator, the field order, or the float format without also updating
// the Python serialiser.
// =============================================================================

const fieldSep = "|"

// MarshalConditionedDataForHash serialises a compute.ConditionedData
// deterministically. Fields appear in the order declared in compute.types.go.
func MarshalConditionedDataForHash(d compute.ConditionedData) string {
	return strings.Join([]string{
		fmt.Sprintf("%.10f", d.VibrationMagnitude),
		fmt.Sprintf("%.10f", d.StrainMagnitude),
		fmt.Sprintf("%.10f", d.Temperature),
		fmt.Sprintf("%.10f", d.Humidity),
		fmt.Sprintf("%.10f", d.AirQualityPM),
		fmt.Sprintf("%.10f", d.OccupancyRatio),
		fmt.Sprintf("%.10f", d.ElectricalLoad),
		fmt.Sprintf("%.10f", d.WaterConsumption),
		fmt.Sprintf("%.10f", d.UptimeContinuity),
		fmt.Sprintf("%.10f", d.CrossSensorConsistency),
		fmt.Sprintf("%.10f", d.CalibrationRecency),
		fmt.Sprintf("%.10f", d.TamperScore),
		fmt.Sprintf("%.10f", d.ChronologicalAge),
		fmt.Sprintf("%.10f", d.MaintenanceSensitivity),
		fmt.Sprintf("%.10f", d.ConditionQuality),
	}, fieldSep)
}

// MarshalIndicatorsForHash serialises a compute.TechnicalIndicators
// deterministically. Fields appear in the order declared in compute.types.go.
func MarshalIndicatorsForHash(i compute.TechnicalIndicators) string {
	return strings.Join([]string{
		fmt.Sprintf("%.10f", i.SHF),
		fmt.Sprintf("%.10f", i.ESF),
		fmt.Sprintf("%.10f", i.USS),
		fmt.Sprintf("%.10f", i.PDP),
		fmt.Sprintf("%.10f", i.CI),
	}, fieldSep)
}

// MarshalTokenForHash serialises a ValuationToken's fields up to (but
// excluding) TokenHash itself, in declaration order. This is the input
// to HashToken.
func MarshalTokenForHash(t ValuationToken) string {
	return strings.Join([]string{
		fmt.Sprintf("%d", t.TokenID),
		t.PropertyID,
		fmt.Sprintf("%d", t.Timestamp),
		t.ConditionedDataHash,
		t.IndicatorsHash,
		fmt.Sprintf("%.10f", t.RTPMV),
		t.Currency,
		t.PrecedingHash,
	}, fieldSep)
}

// =============================================================================
// SHA-256 hashing
// =============================================================================

// HashConditionedData returns the hex SHA-256 of a ConditionedData,
// using MarshalConditionedDataForHash as the input byte sequence.
func HashConditionedData(d compute.ConditionedData) string {
	return sha256Hex(MarshalConditionedDataForHash(d))
}

// HashIndicators returns the hex SHA-256 of a TechnicalIndicators,
// using MarshalIndicatorsForHash as the input byte sequence.
func HashIndicators(i compute.TechnicalIndicators) string {
	return sha256Hex(MarshalIndicatorsForHash(i))
}

// HashToken returns the hex SHA-256 of a ValuationToken, using
// MarshalTokenForHash as the input byte sequence. The TokenHash field
// of the input is ignored — callers typically pass a token with
// TokenHash unset, then store the returned value into TokenHash.
func HashToken(t ValuationToken) string {
	return sha256Hex(MarshalTokenForHash(t))
}

// sha256Hex computes SHA-256 of the input string and returns lowercase
// hex (64 characters).
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
