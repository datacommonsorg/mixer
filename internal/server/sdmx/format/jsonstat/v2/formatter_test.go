package jsonstatv2

import (
	"encoding/json"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
)

func TestJSONStatFormatterUsesShapeDimensionOrder(t *testing.T) {
	formatter := &JSONStatFormatter{}
	output, err := formatter.Format(testSdmxResult(
		[]string{"destinationCountry", "sourceCountry"},
		[]*sdmxpb.SdmxTimeSeries{
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured:  "Count_Person",
					"destinationCountry":                   "country/CAN",
					"sourceCountry":                        "country/USA",
					datacommons.ComponentUnit:              "Person",
					datacommons.ComponentMeasurementMethod: "Census",
					datacommons.ComponentObservationPeriod: "P1Y",
					datacommons.ComponentProvenance:        "dc/base",
					"extraDimension":                       "dropped",
				},
				Attributes: map[string]string{
					datacommons.ComponentScalingFactor: "0",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2021", ObservationValue: "2"},
					{TimePeriod: "2020", ObservationValue: "1"},
				},
			},
		},
	))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	wantID := []interface{}{
		datacommons.ComponentVariableMeasured,
		"destinationCountry",
		"sourceCountry",
		datacommons.ComponentUnit,
		datacommons.ComponentMeasurementMethod,
		datacommons.ComponentObservationPeriod,
		datacommons.ComponentProvenance,
		datacommons.ComponentTimePeriod,
	}
	if diff := cmp.Diff(wantID, got["id"]); diff != "" {
		t.Fatalf("id mismatch (-want +got):\n%s", diff)
	}

	dimensions := got["dimension"].(map[string]interface{})
	if _, ok := dimensions["extraDimension"]; ok {
		t.Fatal("Format() inferred an extra dimension outside result.Shape")
	}

	timePeriod := dimensions[datacommons.ComponentTimePeriod].(map[string]interface{})
	category := timePeriod["category"].(map[string]interface{})
	if diff := cmp.Diff([]interface{}{"2020", "2021"}, category["index"]); diff != "" {
		t.Fatalf("TIME_PERIOD categories mismatch (-want +got):\n%s", diff)
	}

	extension := got["extension"].(map[string]interface{})
	annotations := extension["annotations"].(map[string]interface{})
	provenance := annotations["dc/base"].(map[string]interface{})
	if got := provenance[datacommons.ComponentScalingFactor]; got != "0" {
		t.Fatalf("scalingFactor annotation = %v, want 0", got)
	}
}

func TestJSONStatFormatterMissingShapeReturnsError(t *testing.T) {
	formatter := &JSONStatFormatter{}
	_, err := formatter.Format(&sdmxpb.SdmxDataResult{})
	if err == nil {
		t.Fatal("Format() error = nil, want error")
	}
	if got, want := err.Error(), "SDMX data shape is required"; got != want {
		t.Fatalf("Format() error = %q, want %q", got, want)
	}
}

func TestJSONStatFormatterIndexesTimePeriodByShapePosition(t *testing.T) {
	formatter := &JSONStatFormatter{}
	result := &sdmxpb.SdmxDataResult{
		Shape: &sdmxpb.SdmxDataShape{
			Components: []*sdmxpb.SdmxComponent{
				{Id: datacommons.ComponentVariableMeasured, Kind: sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION},
				{Id: datacommons.ComponentTimePeriod, Kind: sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION},
				{Id: datacommons.ComponentObservationAbout, Kind: sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION},
				{Id: datacommons.ComponentObservationValue, Kind: sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE},
			},
		},
		Series: []*sdmxpb.SdmxTimeSeries{
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured: "Count_Person",
					datacommons.ComponentObservationAbout: "country/CAN",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2020", ObservationValue: "1"},
					{TimePeriod: "2021", ObservationValue: "2"},
				},
			},
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured: "Count_Person",
					datacommons.ComponentObservationAbout: "country/USA",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2020", ObservationValue: "3"},
					{TimePeriod: "2021", ObservationValue: "4"},
				},
			},
		},
	}

	output, err := formatter.Format(result)
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if diff := cmp.Diff([]interface{}{float64(1), float64(3), float64(2), float64(4)}, got["value"]); diff != "" {
		t.Fatalf("value mismatch (-want +got):\n%s", diff)
	}
}

func testSdmxResult(observationProperties []string, series []*sdmxpb.SdmxTimeSeries) *sdmxpb.SdmxDataResult {
	components := datacommons.DataComponentsForObservationProperties(observationProperties)
	result := &sdmxpb.SdmxDataResult{
		Shape: &sdmxpb.SdmxDataShape{
			Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
		},
		Series: series,
	}
	for _, component := range components {
		result.Shape.Components = append(result.Shape.Components, &sdmxpb.SdmxComponent{
			Id:   component.ID,
			Kind: testProtoComponentKind(component.Kind),
		})
	}
	return result
}

func testProtoComponentKind(kind datacommons.ComponentKind) sdmxpb.SdmxComponentKind {
	switch kind {
	case datacommons.ComponentKindDimension:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION
	case datacommons.ComponentKindMeasure:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE
	case datacommons.ComponentKindAttribute:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE
	default:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_UNSPECIFIED
	}
}
