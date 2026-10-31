// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrapV2BackendError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "invalid argument keeps its message",
			err:      status.Error(codes.InvalidArgument, "unsupported target"),
			wantCode: codes.InvalidArgument,
			wantMsg:  "ctx: unsupported target",
		},
		{
			name:     "wrapped status errors are classified by their code",
			err:      fmt.Errorf("resolve: %w", status.Error(codes.InvalidArgument, "unsupported target")),
			wantCode: codes.InvalidArgument,
			wantMsg:  "ctx: unsupported target",
		},
		{
			name:     "transient errors get the retry message",
			err:      status.Error(codes.NotFound, "Session not found: projects/p/instances/i/databases/d/sessions/s"),
			wantCode: codes.NotFound,
			wantMsg:  "ctx: " + v2BackendRetryMessage,
		},
		{
			name:     "non-status errors become Internal with the retry message",
			err:      errors.New("spanner: connection reset"),
			wantCode: codes.Internal,
			wantMsg:  "ctx: " + v2BackendRetryMessage,
		},
		{
			name:     "deployment problems get the do-not-retry message",
			err:      status.Error(codes.FailedPrecondition, "Embeddings service client is not initialized"),
			wantCode: codes.FailedPrecondition,
			wantMsg:  "ctx: " + v2BackendUnsupportedMessage,
		},
		{
			name:     "backend credential errors become Internal with the do-not-retry message",
			err:      status.Error(codes.Unauthenticated, `auth: "invalid_grant" https://us-central1-aiplatform.googleapis.com/v1/projects/p`),
			wantCode: codes.Internal,
			wantMsg:  "ctx: " + v2BackendUnsupportedMessage,
		},
		{
			name:     "timeouts get the timeout message",
			err:      context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "ctx: " + v2BackendTimeoutMessage,
		},
		{
			name:     "cancellation gets the timeout message",
			err:      context.Canceled,
			wantCode: codes.Canceled,
			wantMsg:  "ctx: " + v2BackendTimeoutMessage,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := status.Convert(wrapV2BackendError(tc.err, "ctx"))
			if st.Code() != tc.wantCode || st.Message() != tc.wantMsg {
				t.Errorf("wrapV2BackendError() = (%v, %q), want (%v, %q)", st.Code(), st.Message(), tc.wantCode, tc.wantMsg)
			}
		})
	}
}

func TestV2GetObservationsErrorMessages(t *testing.T) {
	errCases := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "empty variable_dcids",
			err:      errV2NoVariableDcids(),
			wantCode: codes.InvalidArgument,
			wantMsg:  "'variable_dcids' cannot be empty: pass 1 to 10 variable DCIDs returned by search_indicators, e.g. ['Count_Person']",
		},
		{
			name:     "too many variable_dcids",
			err:      errV2TooManyVariableDcids(11),
			wantCode: codes.InvalidArgument,
			wantMsg:  "too many variable_dcids (11): pass at most 10 per call and split the rest into parallel calls",
		},
		{
			name:     "too many child variable_dcids",
			err:      errV2TooManyChildVariableDcids(3),
			wantCode: codes.InvalidArgument,
			wantMsg:  "too many variable_dcids (3) with child_entity_type_dcid: pass at most 2 per call and split the rest into parallel calls",
		},
		{
			name:     "source_override with multiple variables",
			err:      errV2SourceOverrideNeedsOneVariable(2),
			wantCode: codes.InvalidArgument,
			wantMsg:  "source_override applies to a single variable's dataset facet: pass 1 variable in variable_dcids (got 2), or make separate calls per variable",
		},
		{
			name:     "topic as variable_dcids",
			err:      errV2TopicAsVariableDcids([]string{"dc/topic/Health"}),
			wantCode: codes.InvalidArgument,
			wantMsg:  `variable_dcids ["dc/topic/Health"] are topics, not statistical variables: pass topic_dcids to search_indicators to browse their member_variables, then pass variable DCIDs to get_observations`,
		},
		{
			name:     "unknown variable_dcids",
			err:      errV2UnknownVariableDcids([]string{"NotAVar"}),
			wantCode: codes.InvalidArgument,
			wantMsg:  `unknown variable_dcids ["NotAVar"]: pass variable DCIDs returned by search_indicators, not guessed IDs or names`,
		},
		{
			name:     "mixed observation_properties",
			err:      errV2MixedObservationProperties(`Count_Person: ["observationAbout"]; Count_Migration: ["destinationCountry" "sourceCountry"]`),
			wantCode: codes.InvalidArgument,
			wantMsg:  `variable_dcids have different observation_properties (Count_Person: ["observationAbout"]; Count_Migration: ["destinationCountry" "sourceCountry"]): group variables with the same observation_properties, or make separate get_observations calls`,
		},
		{
			name:     "empty entity_dcids",
			err:      errV2NoEntityDcids(),
			wantCode: codes.InvalidArgument,
			wantMsg:  "'entity_dcids' cannot be empty: pass 1 to 20 entity DCIDs returned by resolve_entities, e.g. ['geoId/06']",
		},
		{
			name:     "reserved entity_property",
			err:      errV2ReservedEntityProperty("TIME_PERIOD"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `invalid observation property "TIME_PERIOD": pass an observation property from search_indicators' observation_properties (default 'observationAbout')`,
		},
		{
			name:     "duplicate entity_property",
			err:      errV2DuplicateEntityProperty("donor"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `observation property "donor" is constrained by both entity_property (default 'observationAbout') and additional_entities: pass its DCIDs in entity_dcids (or set entity_property to another property), and use additional_entities for other observation_properties`,
		},
		{
			name:     "too many additional_entities properties",
			err:      errV2TooManyAdditionalEntityProps(3),
			wantCode: codes.InvalidArgument,
			wantMsg:  "too many properties in additional_entities (3): pass at most 2 properties alongside entity_property",
		},
		{
			name:     "empty additional_entities slice",
			err:      errV2EmptyAdditionalEntityDcids("recipient"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `additional_entities["recipient"] cannot be empty: provide 1 to 20 entity DCIDs from resolve_entities, or omit "recipient" from additional_entities to leave it unconstrained`,
		},
		{
			name:     "too many additional_entities DCIDs",
			err:      errV2TooManyAdditionalEntityDcids("recipient", 21),
			wantCode: codes.InvalidArgument,
			wantMsg:  `too many DCIDs in additional_entities["recipient"] (21): pass at most 20 per call and split the rest into additional calls`,
		},
		{
			name:     "wrong observation property",
			err:      errV2WrongObservationProperty("Amount_ODA", []string{"observationAbout"}, []string{"donor", "recipient"}),
			wantCode: codes.InvalidArgument,
			wantMsg:  `variable "Amount_ODA" uses observation_properties ["donor" "recipient"], not ["observationAbout"]: set entity_property to one of ["donor" "recipient"] for entity_dcids (and pass any other constrained properties in additional_entities)`,
		},
		{
			name:     "invalid date",
			err:      errV2InvalidDate("yesterday"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `invalid date "yesterday": use 'latest', 'all', 'range', or an ISO date (YYYY, YYYY-MM, or YYYY-MM-DD)`,
		},
		{
			name:     "conflicting date and range",
			err:      errV2ConflictingDateRange("latest"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `date_range_start and date_range_end can only be used when date is 'range' (or omitted), got date="latest"`,
		},
		{
			name:     "missing date range bounds",
			err:      errV2MissingDateRangeBounds(),
			wantCode: codes.InvalidArgument,
			wantMsg:  "date='range' requires at least one of date_range_start or date_range_end (YYYY, YYYY-MM, or YYYY-MM-DD)",
		},
		{
			name:     "unbounded child date",
			err:      errV2UnboundedChildDate(),
			wantCode: codes.InvalidArgument,
			wantMsg:  "unbounded historical dates (date='all' or a range without date_range_start) are not supported with child_entity_type_dcid: use date='latest', a specific date (e.g. '2023'), or a bounded range with date_range_start (e.g. date_range_start='2019', date_range_end='2024')",
		},
		{
			name:     "invalid date range bound",
			err:      errV2InvalidDateRangeBound("date_range_start", "2023-99"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `invalid date_range_start "2023-99": expected YYYY, YYYY-MM, or YYYY-MM-DD`,
		},
		{
			name:     "inverted date range",
			err:      errV2InvertedDateRange("2024", "2020"),
			wantCode: codes.InvalidArgument,
			wantMsg:  `date_range_start ("2024") cannot be after date_range_end ("2020")`,
		},
		{
			name:     "observations backend failed",
			err:      errV2ObservationsFailed(status.Error(codes.Unavailable, "spanner down")),
			wantCode: codes.Unavailable,
			wantMsg:  "failed to fetch observations: " + v2BackendRetryMessage,
		},
		{
			name:     "observations unavailable fallback",
			err:      errV2ObservationsUnavailable(),
			wantCode: codes.Unavailable,
			wantMsg:  "observations could not be fetched right now; retry the call shortly, or if child_entity_type_dcid was set, do not retry if it fails again",
		},
	}

	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			st := status.Convert(tc.err)
			if st.Code() != tc.wantCode || st.Message() != tc.wantMsg {
				t.Errorf("error = (%v, %q), want (%v, %q)", st.Code(), st.Message(), tc.wantCode, tc.wantMsg)
			}
			if strings.Contains(strings.ToLower(st.Message()), "data commons") {
				t.Errorf("message %q must not mention Data Commons", st.Message())
			}
		})
	}

	noteCases := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "no observations",
			got:  v2NoObsNote("Count_Person", v2GivenEntitiesScope),
			want: "no observations found for Count_Person on the given entity_dcids.",
		},
		{
			name: "no observations with additional_entities",
			got:  v2NoObsNote("Amount_ODA", v2GivenEntitiesAndAdditionalScope),
			want: "no observations found for Amount_ODA on the given entity_dcids and additional_entities.",
		},
		{
			name: "no observations for source_override",
			got:  v2NoObsForSourceNote("Count_Person", "County entities in geoId/06", "12345"),
			want: "no observations found for Count_Person on County entities in geoId/06 with source_override '12345'.",
		},
		{
			name: "no observations for child mode with additional_entities",
			got:  v2NoObsNote("Count_FarmShipment", v2ChildAndAdditionalEntitiesScope("geoId/06", "County")),
			want: "no observations found for Count_FarmShipment on County entities in geoId/06 with the given additional_entities.",
		},
		{
			name: "no observations in date window",
			got:  v2NoObsInDateWindowNote("Count_Person", v2GivenEntitiesScope, "2018", "2023"),
			want: "no observations found across any source for Count_Person on the given entity_dcids in the requested date window (available dates across all sources span 2018 to 2023).",
		},
		{
			name: "no observations for source_override in date window",
			got:  v2NoObsForSourceInDateWindowNote("Count_Person", v2GivenEntitiesScope, "12345", "2018", "2023"),
			want: "no observations found for Count_Person on the given entity_dcids with source_override '12345' in the requested date window (available dates for this source span 2018 to 2023).",
		},
	}
	for _, tc := range noteCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("note = %q, want %q", tc.got, tc.want)
			}
			if strings.Contains(strings.ToLower(tc.got), "data commons") {
				t.Errorf("note %q must not mention Data Commons", tc.got)
			}
		})
	}
}
