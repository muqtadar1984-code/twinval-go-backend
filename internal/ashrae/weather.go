package ashrae

// WeatherStat is one statistical row from the Python `SITE1_WEATHER`
// dict. SeasonalAmp is only populated for parameters that vary across
// the year (currently only AirTempC).
type WeatherStat struct {
	Mean        float64
	Std         float64
	SeasonalAmp float64 // 0 when the parameter is non-seasonal
}

// WeatherProfile collects the six tropical-climate statistical rows
// the engine samples per hour.
type WeatherProfile struct {
	AirTempC            WeatherStat
	DewTempC            WeatherStat
	HumidityPct         WeatherStat
	WindSpeedMs         WeatherStat
	SeaLevelPressureHpa WeatherStat
	PrecipDepthMm       WeatherStat
}

// SITE1Weather is the Kuala Lumpur weather profile (ASHRAE Climate
// Zone 0A — Tropical Rainforest, calibrated to Subang/WMKK station
// data). Equatorial — minimal seasonal variation.
var SITE1Weather = WeatherProfile{
	AirTempC:            WeatherStat{Mean: 27.2, Std: 1.8, SeasonalAmp: 1.2},
	DewTempC:            WeatherStat{Mean: 23.5, Std: 1.4},
	HumidityPct:         WeatherStat{Mean: 82.0, Std: 7.5},
	WindSpeedMs:         WeatherStat{Mean: 2.1, Std: 1.2},
	SeaLevelPressureHpa: WeatherStat{Mean: 1009.8, Std: 3.2},
	PrecipDepthMm:       WeatherStat{Mean: 7.5, Std: 14.2},
}

// WeatherReading is one sampled hour of weather output from the
// engine. Units match the Python source so the values are directly
// usable downstream.
type WeatherReading struct {
	AirTemperatureC     float64
	DewTemperatureC     float64
	HumidityPct         float64
	WindSpeedMs         float64
	SeaLevelPressureHpa float64
	PrecipDepthMm       float64
}
