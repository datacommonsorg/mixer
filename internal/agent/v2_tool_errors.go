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

// This file holds the errors (and non-fatal notes) that the agent service's v2 methods return to their
// callers, which are typically LLM agents using the MCP tools: the text is shown to the agent in the
// tool result.
//
// Keep every caller-visible v2 agent error here so the full set can be reviewed and tuned together,
// like the tool and field descriptions. Messages should say what was wrong and how to fix the call
// (limits, expected format, an example). Internal errors that are only logged do not belong here.

package agent

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolve_entities: request validation.

func errV2NoEntities() error {
	return status.Error(codes.InvalidArgument,
		`'entities' cannot be empty: provide at least one entity to resolve, e.g. [{"name": "California"}]`)
}

func errV2TooManyEntities(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many entities (%d): resolve at most %d entities per call and split the rest into additional calls",
		count, v2MaxEntitiesPerRequest)
}

func errV2EmptyEntityName(index int) error {
	return status.Errorf(codes.InvalidArgument,
		"entities[%d].name cannot be empty: provide the entity name to resolve, e.g. 'California' or 'FEMA'", index)
}

func errV2TooManyChildHints(index, count int) error {
	return status.Errorf(codes.InvalidArgument,
		"entities[%d].child_entity_type_hints has %d hints: use at most %d specific types, most specific first, e.g. ['county', 'administrative area 1']",
		index, count, v2MaxChildHintsPerEntity)
}

func errV2InvalidTarget(target string) error {
	return status.Errorf(codes.InvalidArgument, "invalid target %q, must be one of %v", target, validTargets)
}

// resolve_entities: notes. These are returned in successful responses for non-fatal problems.

// v2ChildTypesUnavailableNote is returned when the child entity type lookup fails, in place of the child
// entity types table.
const v2ChildTypesUnavailableNote = "child entity types could not be looked up (the lookup failed or timed out); the resolved entities are still valid. Retry resolve_entities once with the same child_entity_type_hints; do not resolve individual children instead."

// resolve_entities: backend failures.

func errV2ResolveFailed(err error, name, resolver string) error {
	return wrapV2BackendError(err, fmt.Sprintf("failed to resolve %q with resolver %q", name, resolver))
}

func errV2TypeHintsFailed(err error) error {
	return wrapV2BackendError(err, "failed to resolve entity_type_hints and child_entity_type_hints")
}

// search_indicators: request validation.

func errV2QueryAndTopicDcids() error {
	return status.Error(codes.InvalidArgument,
		"pass either 'query' (to search) or 'topic_dcids' (to browse those topics), not both")
}

func errV2TooManyEntityDcids(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many entity_dcids (%d): pass at most %d per call and split the rest into additional calls",
		count, v2MaxEntityDcids)
}

func errV2TooManyTopicDcids(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many topic_dcids (%d): pass at most %d per call", count, v2MaxTopicDcids)
}

func errV2UnknownEntityDcids(dcids []string) error {
	return status.Errorf(codes.InvalidArgument,
		"unknown entity_dcids %q: pass DCIDs returned by resolve_entities (e.g. 'geoId/06'), not names", dcids)
}

func errV2UnknownTopicDcids(dcids []string) error {
	return status.Errorf(codes.InvalidArgument,
		"unknown topic_dcids %q: pass topic DCIDs from search_indicators results (a topic's dcid or its member_topics)", dcids)
}

func errV2ChildModeNeedsOneEntity(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"child_entity_type_dcid needs exactly one parent DCID in entity_dcids (got %d); make one call per parent", count)
}

func errV2NoChildEntitiesOfType(parent, childType string) error {
	return status.Errorf(codes.InvalidArgument,
		"no entities of type %q under %q: use a child_entity_type_dcid from resolve_entities' candidate_child_entity_types for this parent",
		childType, parent)
}

// search_indicators: notes. These are returned in successful responses.
//
// Empty-result notes state only what was checked. They deliberately give no retry guidance: agents decide
// on their own whether to rephrase, and hints about where else data might be tend to send them looping.

// Scopes for the empty-result notes: what the results were filtered by.
const v2GivenEntitiesScope = "the given entity_dcids"

// v2ChildEntitiesScope is the note scope in child mode, e.g. "County entities in geoId/06".
func v2ChildEntitiesScope(parent, childType string) string {
	return fmt.Sprintf("%s entities in %s", childType, parent)
}

// v2NoEntitiesNote is returned when no entity_dcids were given, so results were not filtered.
const v2NoEntitiesNote = "results are not filtered by data availability, which varies by entity; pass entity_dcids from resolve_entities to keep only indicators with data for those entities"

// v2NoMatchNote is returned when a search matched no topics or variables at all.
const v2NoMatchNote = "no topics or variables match this query."

// Empty-result notes for when the entities filtered out every result. scope is what the results were
// filtered by (v2GivenEntitiesScope or v2ChildEntitiesScope).

// v2NoEntityDataNote: none of the given entities has observation data for any variable. Child mode never
// uses it, since a few sampled children can't show that a whole type has no data.
const v2NoEntityDataNote = "there is no data for the given entity_dcids."

// v2NoQueryDataNote: the search matched topics or variables, but none has data for the scope.
func v2NoQueryDataNote(scope string) string {
	return "none of the topics or variables matching this query have data for " + scope + "."
}

// v2NoRequestedTopicDataNote: none of the requested topics has data for the scope.
func v2NoRequestedTopicDataNote(scope string) string {
	return "none of the requested topics have data for " + scope + "."
}

// v2NoTopLevelTopicDataNote: none of the top-level topics has data for the scope.
func v2NoTopLevelTopicDataNote(scope string) string {
	return "no topics have data for " + scope + "."
}

// search_indicators: backend failures.

func errV2SearchFailed(err error) error {
	return wrapV2BackendError(err, "failed to search indicators")
}

func errV2TopicLookupFailed(err error) error {
	return wrapV2BackendError(err, "failed to look up topics")
}

func errV2EntityLookupFailed(err error) error {
	return wrapV2BackendError(err, "failed to look up entity_dcids")
}

func errV2AvailabilityFailed(err error) error {
	return wrapV2BackendError(err, "failed to check which indicators have data for entity_dcids")
}

// errV2ChildEntitySamplingFailed is returned when sampling child entities times out.
func errV2ChildEntitySamplingFailed(err error) error {
	return wrapV2BackendError(err, "failed to sample child entities")
}

// errV2ChildEntitiesUnavailable is returned when child entities can't be sampled for any reason other than a
// timeout. Unlike the other backend failures it is agent-owned rather than built by wrapV2BackendError: the
// sample can come from the LinkedEdge table, which a misconfigured deployment may lack, and
// wrapV2BackendError passes InvalidArgument messages through, which would expose Spanner's "Table not
// found: LinkedEdge" to the caller. A missing table is permanent, so the message says not to retry; other
// failures get the same message rather than a guess at whether they are temporary. The raw error is logged
// by the caller.
func errV2ChildEntitiesUnavailable() error {
	return status.Error(codes.Unavailable,
		"filtering by child entities is not available right now; do not retry with child_entity_type_dcid")
}

// get_observations: request validation.

func errV2NoVariableDcids() error {
	return status.Errorf(codes.InvalidArgument,
		"'variable_dcids' cannot be empty: pass 1 to %d variable DCIDs returned by search_indicators, e.g. ['Count_Person']",
		v2MaxVariableDcids)
}

func errV2TooManyVariableDcids(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many variable_dcids (%d): pass at most %d per call and split the rest into parallel calls",
		count, v2MaxVariableDcids)
}

func errV2TooManyChildVariableDcids(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many variable_dcids (%d) with child_entity_type_dcid: pass at most %d per call and split the rest into parallel calls",
		count, v2MaxChildVariableDcids)
}

func errV2SourceOverrideNeedsOneVariable(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"source_override applies to a single variable's dataset facet: pass 1 variable in variable_dcids (got %d), or make separate calls per variable",
		count)
}

func errV2TopicAsVariableDcids(dcids []string) error {
	return status.Errorf(codes.InvalidArgument,
		"variable_dcids %q are topics, not statistical variables: pass topic_dcids to search_indicators to browse their member_variables, then pass variable DCIDs to get_observations",
		dcids)
}

func errV2UnknownVariableDcids(dcids []string) error {
	return status.Errorf(codes.InvalidArgument,
		"unknown variable_dcids %q: pass variable DCIDs returned by search_indicators, not guessed IDs or names",
		dcids)
}

func errV2MixedObservationProperties(summary string) error {
	return status.Errorf(codes.InvalidArgument,
		"variable_dcids have different observation_properties (%s): group variables with the same observation_properties, or make separate get_observations calls",
		summary)
}

func errV2NoEntityDcids() error {
	return status.Errorf(codes.InvalidArgument,
		"'entity_dcids' cannot be empty: pass 1 to %d entity DCIDs returned by resolve_entities, e.g. ['geoId/06']",
		v2MaxEntityDcids)
}

func errV2ReservedEntityProperty(prop string) error {
	return status.Errorf(codes.InvalidArgument,
		"invalid observation property %q: pass an observation property from search_indicators' observation_properties (default 'observationAbout')",
		prop)
}

func errV2DuplicateEntityProperty(prop string) error {
	return status.Errorf(codes.InvalidArgument,
		"observation property %q is constrained by both entity_property (default 'observationAbout') and additional_entities: pass its DCIDs in entity_dcids (or set entity_property to another property), and use additional_entities for other observation_properties",
		prop)
}

func errV2TooManyAdditionalEntityProps(count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many properties in additional_entities (%d): pass at most %d properties alongside entity_property",
		count, v2MaxAdditionalEntityProps)
}

func errV2EmptyAdditionalEntityDcids(prop string) error {
	return status.Errorf(codes.InvalidArgument,
		"additional_entities[%q] cannot be empty: provide 1 to %d entity DCIDs from resolve_entities, or omit %q from additional_entities to leave it unconstrained",
		prop, v2MaxEntityDcids, prop)
}

func errV2TooManyAdditionalEntityDcids(prop string, count int) error {
	return status.Errorf(codes.InvalidArgument,
		"too many DCIDs in additional_entities[%q] (%d): pass at most %d per call and split the rest into additional calls",
		prop, count, v2MaxEntityDcids)
}

func errV2WrongObservationProperty(variable string, badProps, allowedProps []string) error {
	return status.Errorf(codes.InvalidArgument,
		"variable %q uses observation_properties %q, not %q: set entity_property to one of %q for entity_dcids (and pass any other constrained properties in additional_entities)",
		variable, allowedProps, badProps, allowedProps)
}

func errV2InvalidDate(date string) error {
	return status.Errorf(codes.InvalidArgument,
		"invalid date %q: use 'latest', 'all', 'range', or an ISO date (YYYY, YYYY-MM, or YYYY-MM-DD)", date)
}

func errV2ConflictingDateRange(date string) error {
	return status.Errorf(codes.InvalidArgument,
		"date_range_start and date_range_end can only be used when date is 'range' (or omitted), got date=%q", date)
}

func errV2MissingDateRangeBounds() error {
	return status.Error(codes.InvalidArgument,
		"date='range' requires at least one of date_range_start or date_range_end (YYYY, YYYY-MM, or YYYY-MM-DD)")
}

func errV2UnboundedChildDate() error {
	return status.Error(codes.InvalidArgument,
		"unbounded historical dates (date='all' or a range without date_range_start) are not supported with child_entity_type_dcid: use date='latest', a specific date (e.g. '2023'), or a bounded range with date_range_start (e.g. date_range_start='2019', date_range_end='2024')")
}

func errV2InvalidDateRangeBound(field, val string) error {
	return status.Errorf(codes.InvalidArgument,
		"invalid %s %q: expected YYYY, YYYY-MM, or YYYY-MM-DD", field, val)
}

func errV2InvertedDateRange(start, end string) error {
	return status.Errorf(codes.InvalidArgument,
		"date_range_start (%q) cannot be after date_range_end (%q)", start, end)
}

// get_observations: notes. These are returned on individual variable entries when Data has no rows.
// Like search_indicators notes, they state only what was checked and give no retry advice.

// v2GivenEntitiesAndAdditionalScope is the note scope in explicit mode when additional_entities is also set.
const v2GivenEntitiesAndAdditionalScope = "the given entity_dcids and additional_entities"

// v2ChildAndAdditionalEntitiesScope is the note scope in child mode when additional_entities is also set.
func v2ChildAndAdditionalEntitiesScope(parent, childType string) string {
	return fmt.Sprintf("%s entities in %s with the given additional_entities", childType, parent)
}

func v2NoObsNote(variable, scope string) string {
	return fmt.Sprintf("no observations found for %s on %s.", variable, scope)
}

func v2NoObsForSourceNote(variable, scope, sourceID string) string {
	return fmt.Sprintf("no observations found for %s on %s with source_override '%s'.", variable, scope, sourceID)
}

func v2NoObsInDateWindowNote(variable, scope, minDate, maxDate string) string {
	return fmt.Sprintf(
		"no observations found across any source for %s on %s in the requested date window (available dates across all sources span %s to %s).",
		variable, scope, minDate, maxDate)
}

func v2NoObsForSourceInDateWindowNote(variable, scope, sourceID, minDate, maxDate string) string {
	return fmt.Sprintf(
		"no observations found for %s on %s with source_override '%s' in the requested date window (available dates for this source span %s to %s).",
		variable, scope, sourceID, minDate, maxDate)
}

// get_observations: backend failures.

func errV2ObservationsFailed(err error) error {
	return wrapV2BackendError(err, "failed to fetch observations")
}

// errV2ObservationsUnavailable is returned when SdmxData fails with an internal InvalidArgument (for
// example, Spanner's "Table not found: LinkedEdge" on a misconfigured deployment) after variable and
// observation_properties checks pass. Like errV2ChildEntitiesUnavailable, it is agent-owned so
// wrapV2BackendError never exposes raw Spanner or SDMX messages to the caller.
func errV2ObservationsUnavailable() error {
	return status.Error(codes.Unavailable,
		"observations could not be fetched right now; retry the call shortly, or if child_entity_type_dcid was set, do not retry if it fails again")
}

// Generic caller-facing replacements for backend error details, which are not actionable for the caller
// and can expose internal endpoints, resource paths and project IDs. Details are logged by the callers.
// They don't name the product, since Data Commons Platform deployments may not present themselves as
// Data Commons.
const (
	v2BackendRetryMessage       = "a backend service is temporarily unavailable; retry the call shortly"
	v2BackendTimeoutMessage     = "a backend service timed out; retry the call shortly"
	v2BackendUnsupportedMessage = "this operation is not available in this deployment; do not retry it"
)

// wrapV2BackendError adds caller-facing context to a backend error and replaces backend details with a
// generic message chosen by gRPC code (non-status errors count as Internal):
//   - InvalidArgument keeps its message: backends use it to reject request values (e.g. an unsupported
//     target) and it is trusted to be safe and actionable for the caller.
//   - DeadlineExceeded and Canceled get the timeout message.
//   - Unimplemented, FailedPrecondition, PermissionDenied and Unauthenticated are deployment or
//     configuration problems that retrying will not fix. Backend auth codes are returned as Internal, since
//     they concern the server's credentials, not the caller's.
//   - Everything else (e.g. Unavailable, Internal, NotFound for transient Spanner sessions) gets the retry
//     message.
//
// It does not log; callers log the original error.
func wrapV2BackendError(err error, msg string) error {
	st := v2BackendStatus(err)
	switch st.Code() {
	case codes.InvalidArgument:
		return status.Errorf(st.Code(), "%s: %s", msg, st.Message())
	case codes.DeadlineExceeded, codes.Canceled:
		return status.Errorf(st.Code(), "%s: %s", msg, v2BackendTimeoutMessage)
	case codes.Unimplemented, codes.FailedPrecondition:
		return status.Errorf(st.Code(), "%s: %s", msg, v2BackendUnsupportedMessage)
	case codes.PermissionDenied, codes.Unauthenticated:
		return status.Errorf(codes.Internal, "%s: %s", msg, v2BackendUnsupportedMessage)
	default:
		return status.Errorf(st.Code(), "%s: %s", msg, v2BackendRetryMessage)
	}
}

// v2BackendStatus returns the gRPC status of a backend error: context errors map to their codes, wrapped
// errors keep their innermost status, and other errors count as Internal.
func v2BackendStatus(err error) *status.Status {
	st := status.FromContextError(err)
	if st.Code() != codes.Unknown {
		return st
	}
	// errors.As finds the innermost status, so wrapped errors keep only the status message (status.FromError
	// would use the full wrapped text).
	var withStatus interface{ GRPCStatus() *status.Status }
	if errors.As(err, &withStatus) {
		return withStatus.GRPCStatus()
	}
	return status.New(codes.Internal, err.Error())
}
