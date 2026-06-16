package sdmx

const (
	JSONStatContentType = "application/json; charset=utf-8"
	CSVContentType      = "application/vnd.sdmx.data+csv;version=2.0.0"

	DataContext    = "dataflow"
	DataAgencyID   = "DATACOMMONS"
	DataResourceID = "DF_OBSERVATIONS"
	DataVersion    = "1.0.0"

	DimVariableMeasured  = "variableMeasured"
	DimObservationDate   = "TIME_PERIOD"
	DimObservationValue  = "OBS_VALUE"
	FallbackNotAvailable = "NotApplicable" // Used across datasets to represent missing constraints.
	ParamStartPeriod     = "startPeriod"
	ParamEndPeriod       = "endPeriod"
)
