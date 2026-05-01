package ashrae

import (
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/twinval/internal/condition"
)

// BaseTime is the start of the ASHRAE GEPIII dataset, matching the
// Python source's `datetime(2016, 1, 1)`.
var BaseTime = time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)

// SensorEngine is the per-building synthetic-data generator. Each
// engine maintains its own RNG and hour cursor; calling Next advances
// the simulated clock by exactly one hour and returns one reading.
//
// SensorEngine is not goroutine-safe. Use one engine per goroutine, or
// guard externally.
type SensorEngine struct {
	Building  Building
	Weather   WeatherProfile
	rng       *rand.Rand
	hourIndex int
}

// NewEngine constructs an engine seeded from Building.ID. hourOffset
// shifts the simulated clock; pass 0 to start at BaseTime, or a value
// like 14 to start at 14:00 on day 1.
func NewEngine(b Building, w WeatherProfile, hourOffset int) *SensorEngine {
	src := rand.NewPCG(b.ID, b.ID*2+1)
	return &SensorEngine{
		Building:  b,
		Weather:   w,
		rng:       rand.New(src),
		hourIndex: hourOffset,
	}
}

// SensorReading is one full hour of simulated sensor + meter + weather
// data. The Patent-required normalised stress fields (Vibration, Strain,
// etc.) match the Python engine's numerical scale [0, 1], not physical
// units. Use ToConditionedData / ToRawSensorBatch to map into the Go
// pipeline's physical-unit ConditionedData shape.
type SensorReading struct {
	Time      time.Time
	Hour      int // 0-23
	DayOfWeek int // 0=Monday … 6=Sunday (matches Python weekday())
	DayOfYear int // 1-366

	// Patent-required normalised stress sensor values [0, 1].
	Vibration      float64
	Strain         float64
	Moisture       float64
	Temperature    float64 // stress score, NOT °C
	Occupancy      float64
	ElectricalLoad float64
	AirQuality     float64 // already inverted to stress: 1.0 = polluted

	// Raw ASHRAE meter readings (kWh).
	MeterReadings map[string]float64

	// Sampled weather for this hour.
	Weather WeatherReading
}

// Next advances the simulated clock by one hour and returns one
// reading. Subsequent calls produce subsequent hours — call rate is
// independent of wall-clock time.
func (e *SensorEngine) Next() SensorReading {
	t := BaseTime.Add(time.Duration(e.hourIndex) * time.Hour)
	hour := t.Hour()
	pyDow := pythonWeekday(t)
	doy := t.YearDay()
	e.hourIndex++

	// Raw ASHRAE meter readings for this hour. Iterate in sorted order
	// so the RNG consumption sequence is deterministic — Go map iteration
	// would otherwise vary between runs even with the same seed.
	names := make([]string, 0, len(e.Building.MeterProfiles))
	for name := range e.Building.MeterProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	meters := make(map[string]float64, len(names))
	for _, name := range names {
		meters[name] = e.meterReading(e.Building.MeterProfiles[name], hour, pyDow, doy)
	}

	// Weather sample for this hour
	weather := e.weatherReading(doy, hour)

	// Derived sensors — match the Python `get_sensor_reading` body 1:1.
	elecProfile := e.Building.MeterProfiles["electricity_kwh"]
	elec := meters["electricity_kwh"]
	elecCapacity := elecProfile.Mean * 2.5
	electricalLoad := clamp01(elec / elecCapacity)

	occDiurnal := math.Exp(-0.5 * math.Pow((float64(hour)-float64(elecProfile.PeakHour))/3.5, 2))
	occWeekend := 1.0
	if pyDow >= 5 {
		occWeekend = 0.3
	}
	occupancy := clamp01(occDiurnal*occWeekend + e.rng.NormFloat64()*0.05)

	tempStress := clamp01(math.Abs(weather.AirTemperatureC-22.0) / 20.0)

	moistureBase := weather.HumidityPct / 100.0
	dewProximity := math.Max(0, 1-(weather.AirTemperatureC-weather.DewTemperatureC)/10.0)
	precipFactor := math.Min(1.0, weather.PrecipDepthMm/20.0)
	moisture := clamp01(0.5*moistureBase + 0.3*dewProximity + 0.2*precipFactor)

	hvacVibration := electricalLoad * 0.6
	structuralBase := 0.02 + 0.01*(weather.WindSpeedMs/10.0)
	vibration := clamp01(hvacVibration*0.15 + structuralBase + e.rng.NormFloat64()*0.015)

	thermalStrain := tempStress * 0.08
	loadStrain := occupancy * 0.05
	strain := clamp01(thermalStrain + loadStrain + e.rng.NormFloat64()*0.01)

	hvacEfficiency := 1 - electricalLoad*0.3
	aqBase := 1.0 - occupancy*0.4
	airQuality := clamp01(aqBase*hvacEfficiency + e.rng.NormFloat64()*0.03)
	airQualityStress := 1.0 - airQuality

	return SensorReading{
		Time:           t,
		Hour:           hour,
		DayOfWeek:      pyDow,
		DayOfYear:      doy,
		Vibration:      round4(vibration),
		Strain:         round4(strain),
		Moisture:       round4(moisture),
		Temperature:    round4(tempStress),
		Occupancy:      round4(occupancy),
		ElectricalLoad: round4(electricalLoad),
		AirQuality:     round4(airQualityStress),
		MeterReadings:  meters,
		Weather:        weather,
	}
}

// meterReading samples one ASHRAE meter — mirrors Python's
// `_ashrae_meter_reading`.
func (e *SensorEngine) meterReading(p MeterProfile, hour, pyDow, doy int) float64 {
	diurnal := math.Exp(-0.5 * math.Pow((float64(hour)-float64(p.PeakHour))/4.0, 2))
	weekendMult := 1.0
	if pyDow >= 5 {
		weekendMult = p.WeekendFactor
	}
	seasonal := 1.0 + p.SeasonalAmp*math.Sin(2*math.Pi*float64(doy-172)/365)
	base := p.Mean * diurnal * weekendMult * seasonal
	noise := e.rng.NormFloat64() * p.Std * 0.3
	return math.Max(0, base+noise)
}

// weatherReading samples one hour of weather — mirrors Python's
// `_weather_reading`.
func (e *SensorEngine) weatherReading(doy, hour int) WeatherReading {
	wp := e.Weather
	seasonalT := wp.AirTempC.SeasonalAmp * math.Sin(2*math.Pi*float64(doy-172)/365)
	diurnalT := 4.0 * math.Sin(2*math.Pi*(float64(hour)-6)/24)
	airTemp := wp.AirTempC.Mean + seasonalT + diurnalT + e.rng.NormFloat64()*wp.AirTempC.Std*0.3

	humidity := wp.HumidityPct.Mean - 0.8*(airTemp-wp.AirTempC.Mean) + e.rng.NormFloat64()*wp.HumidityPct.Std*0.3
	humidity = clamp(humidity, 20, 100)

	dewTemp := airTemp - ((100 - humidity) / 5.0)

	wind := math.Max(0, wp.WindSpeedMs.Mean+e.rng.NormFloat64()*wp.WindSpeedMs.Std*0.4)
	pressure := wp.SeaLevelPressureHpa.Mean + e.rng.NormFloat64()*wp.SeaLevelPressureHpa.Std

	precip := 0.0
	if e.rng.Float64() < 0.25 {
		precip = e.rng.ExpFloat64() * wp.PrecipDepthMm.Mean * 3
	}

	return WeatherReading{
		AirTemperatureC:     round2(airTemp),
		DewTemperatureC:     round2(dewTemp),
		HumidityPct:         round1(humidity),
		WindSpeedMs:         round2(wind),
		SeaLevelPressureHpa: round1(pressure),
		PrecipDepthMm:       round2(precip),
	}
}

// ToRawSensorBatch maps a SensorReading into the Go pipeline's
// RawSensorBatch shape.
//
// The Python POC's compute layer consumes normalised stress scores;
// the Go compute layer consumes physical units (m/s² for vibration,
// microstrain, °C, %RH, µg/m³). This function projects the engine's
// normalised values back to physical units using the patent-document
// scale anchors:
//
//	vibration  [0, 1] → [0, 0.5]   m/s²       (ceiling 0.05 ≈ normalised 0.10)
//	strain     [0, 1] → [0, 1000]  microstrain (ceiling 200 ≈ normalised 0.20)
//	temperature stress → physical air_temp from the weather sample
//	humidity stress    → physical humidity from the weather sample
//	air_quality stress [0, 1] → PM2.5 [0, 100] µg/m³
//	occupancy / electrical / water → already in [0, 1], passed through
//
// The CalibrationDaysAgo and PropertyMeta come from the caller.
func (r SensorReading) ToRawSensorBatch(propertyID string, meta condition.PropertyMeta, calibrationDaysAgo float64) condition.RawSensorBatch {
	tsNs := r.Time.UnixNano()
	zone := "ashrae"
	mk := func(id string, st condition.SensorType, value float64) condition.RawSensorReading {
		return condition.RawSensorReading{
			SensorID:           id,
			SensorType:         st,
			Zone:               zone,
			TimestampNs:        tsNs,
			Value:              value,
			CalibrationDaysAgo: calibrationDaysAgo,
		}
	}
	// project normalised stress back to physical units
	vibrationPhysical := r.Vibration * 0.5         // m/s²
	strainPhysical := r.Strain * 1000.0            // microstrain
	pmPhysical := r.AirQuality * 100.0             // µg/m³
	temperaturePhysical := r.Weather.AirTemperatureC
	humidityPhysical := r.Weather.HumidityPct
	water := 0.3 + r.ElectricalLoad*0.4 // crude proxy: water tracks utilisation

	return condition.RawSensorBatch{
		PropertyID:                propertyID,
		WindowStartNs:             tsNs - int64(time.Hour),
		WindowEndNs:               tsNs,
		ExpectedReadingsPerSensor: 1,
		PropertyMeta:              meta,
		Readings: []condition.RawSensorReading{
			mk("vib1", condition.SensorVibration, vibrationPhysical),
			mk("str1", condition.SensorStrain, strainPhysical),
			mk("tmp1", condition.SensorTemperature, temperaturePhysical),
			mk("hum1", condition.SensorHumidity, humidityPhysical),
			mk("pm1", condition.SensorPM25, pmPhysical),
			mk("occ1", condition.SensorOccupancy, r.Occupancy),
			mk("el1", condition.SensorElectrical, r.ElectricalLoad),
			mk("wat1", condition.SensorWater, water),
		},
	}
}

// pythonWeekday converts Go's Sunday-anchored Weekday() (Sun=0, Mon=1, …)
// to Python's Monday-anchored datetime.weekday() (Mon=0, …, Sun=6).
func pythonWeekday(t time.Time) int {
	return (int(t.Weekday()) + 6) % 7
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clamp01(v float64) float64 { return clamp(v, 0, 1) }
func round1(v float64) float64  { return math.Round(v*10) / 10 }
func round2(v float64) float64  { return math.Round(v*100) / 100 }
func round4(v float64) float64  { return math.Round(v*10000) / 10000 }
