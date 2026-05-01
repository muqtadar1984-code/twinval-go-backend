// Package ashrae is the Go port of the Python POC's
// `src/data/sensor_engine.py` and `config/buildings.py`.
//
// It generates synthetic sensor readings calibrated to ASHRAE GEPIII
// statistical distributions, applied to five Malaysian commercial
// properties under climate zone 0A (tropical equatorial). The output
// drives the same compute pipeline the Python POC uses.
//
// Every building has its own seeded RNG (math/rand/v2 PCG, seed
// derived from Building.ID), so a re-run with the same starting
// hour offset produces the same sensor stream — useful for
// reproducibility and audit.
//
// PARITY NOTE:
// Random number streams will not be byte-identical to Python (NumPy
// uses its own PCG64 variant with different seeding semantics), but
// the distributions and patterns (diurnal, weekend, seasonal, weather)
// match the Python source. Same buildings, same govt valuations, same
// meter profile parameters.
package ashrae

// MeterProfile mirrors a single ASHRAE meter row from the Python
// `meter_profiles` dict. Used to generate one diurnal/weekend/seasonal
// reading per simulated hour.
type MeterProfile struct {
	Mean          float64 // hourly mean (kWh) at peak hour, no seasonality
	Std           float64 // standard deviation across the dataset
	PeakHour      int     // hour of day (0-23) of maximum demand
	WeekendFactor float64 // multiplier on weekends (1.0 = no change, <1 = lower)
	SeasonalAmp   float64 // amplitude of sinusoidal seasonal swing
}

// Building is the catalogue entry for one simulated property. Mirrors
// `ASHRAE_BUILDINGS[<key>]` from the Python source.
type Building struct {
	ID          uint64 // numeric id, also seeds the per-building RNG
	Key         string // canonical key, e.g. "MY-KL-OFF-KLCC"
	Name        string // human name
	PrimaryUse  string
	SquareFeet  int
	YearBuilt   int
	FloorCount  int
	Latitude    float64
	Longitude   float64
	Address     string
	ClimateZone string

	// Valuation in MYR. StructureValue is the depreciable component
	// that the Health Factor multiplies; LandValue is unchanged.
	GovtValuation  float64
	LandValue      float64
	StructureValue float64
	Currency       string

	MeterProfiles map[string]MeterProfile
}

// Buildings is the canonical list of properties the ASHRAE engine
// simulates. Order is stable; iterate by index for deterministic
// behaviour.
var Buildings = []Building{
	{
		ID: 101, Key: "MY-KL-OFF-KLCC",
		Name: "KLCC Tower", PrimaryUse: "Grade A Office",
		SquareFeet: 125_000, YearBuilt: 1998, FloorCount: 48,
		Latitude: 3.1578, Longitude: 101.7123,
		Address:     "Kuala Lumpur City Centre, 50088 Kuala Lumpur",
		ClimateZone: "0A — Very Hot Humid (Af — Tropical Rainforest)",
		GovtValuation: 185_000_000, LandValue: 75_000_000, StructureValue: 110_000_000,
		Currency: "MYR",
		MeterProfiles: map[string]MeterProfile{
			"electricity_kwh":   {Mean: 3480.5, Std: 620.4, PeakHour: 14, WeekendFactor: 0.22, SeasonalAmp: 0.04},
			"chilled_water_kwh": {Mean: 1920.8, Std: 380.5, PeakHour: 13, WeekendFactor: 0.18, SeasonalAmp: 0.06},
		},
	},
	{
		ID: 102, Key: "MY-SA-IDC-AXIS",
		Name: "Axis Shah Alam DC", PrimaryUse: "Industrial / Data Centre",
		SquareFeet: 48_000, YearBuilt: 2016, FloorCount: 4,
		Latitude: 3.0851, Longitude: 101.5325,
		Address:     "Shah Alam, Selangor 40150",
		ClimateZone: "0A — Very Hot Humid (Af — Tropical Rainforest)",
		GovtValuation: 62_000_000, LandValue: 22_000_000, StructureValue: 40_000_000,
		Currency: "MYR",
		MeterProfiles: map[string]MeterProfile{
			"electricity_kwh":   {Mean: 8650.2, Std: 920.8, PeakHour: 12, WeekendFactor: 0.95, SeasonalAmp: 0.02},
			"chilled_water_kwh": {Mean: 3280.4, Std: 480.2, PeakHour: 12, WeekendFactor: 0.93, SeasonalAmp: 0.02},
		},
	},
	{
		ID: 103, Key: "MY-KL-HC-ALAQAR",
		Name: "Al-Aqar Medical Hub", PrimaryUse: "Healthcare",
		SquareFeet: 32_000, YearBuilt: 2007, FloorCount: 12,
		Latitude: 3.1502, Longitude: 101.6235,
		Address:     "Damansara, 47500 Petaling Jaya, Selangor",
		ClimateZone: "0A — Very Hot Humid (Af — Tropical Rainforest)",
		GovtValuation: 41_000_000, LandValue: 16_000_000, StructureValue: 25_000_000,
		Currency: "MYR",
		MeterProfiles: map[string]MeterProfile{
			"electricity_kwh":   {Mean: 1820.6, Std: 310.8, PeakHour: 11, WeekendFactor: 0.88, SeasonalAmp: 0.03},
			"chilled_water_kwh": {Mean: 820.4, Std: 180.5, PeakHour: 10, WeekendFactor: 0.85, SeasonalAmp: 0.03},
		},
	},
	{
		ID: 104, Key: "MY-KL-RET-PAV",
		Name: "Pavilion Retail Arcade", PrimaryUse: "Retail",
		SquareFeet: 89_000, YearBuilt: 2007, FloorCount: 7,
		Latitude: 3.1489, Longitude: 101.7132,
		Address:     "168 Jalan Bukit Bintang, 55100 Kuala Lumpur",
		ClimateZone: "0A — Very Hot Humid (Af — Tropical Rainforest)",
		GovtValuation: 137_000_000, LandValue: 58_000_000, StructureValue: 79_000_000,
		Currency: "MYR",
		MeterProfiles: map[string]MeterProfile{
			"electricity_kwh":   {Mean: 2750.4, Std: 510.6, PeakHour: 16, WeekendFactor: 1.35, SeasonalAmp: 0.05},
			"chilled_water_kwh": {Mean: 1480.2, Std: 310.4, PeakHour: 15, WeekendFactor: 1.30, SeasonalAmp: 0.05},
		},
	},
	{
		ID: 105, Key: "MY-SJ-LOG-SUN",
		Name: "Sunway Logistics Park", PrimaryUse: "Logistics / Industrial",
		SquareFeet: 71_000, YearBuilt: 2014, FloorCount: 5,
		Latitude: 3.0588, Longitude: 101.5841,
		Address:     "Subang Jaya, 47500 Selangor",
		ClimateZone: "0A — Very Hot Humid (Af — Tropical Rainforest)",
		GovtValuation: 88_000_000, LandValue: 34_000_000, StructureValue: 54_000_000,
		Currency: "MYR",
		MeterProfiles: map[string]MeterProfile{
			"electricity_kwh":   {Mean: 840.8, Std: 195.4, PeakHour: 10, WeekendFactor: 0.40, SeasonalAmp: 0.03},
			"chilled_water_kwh": {Mean: 415.6, Std: 110.2, PeakHour: 11, WeekendFactor: 0.38, SeasonalAmp: 0.03},
		},
	},
}

// ByKey returns the building with the given canonical key. ok=false
// when the key does not match any catalogued building.
func ByKey(key string) (Building, bool) {
	for _, b := range Buildings {
		if b.Key == key {
			return b, true
		}
	}
	return Building{}, false
}
