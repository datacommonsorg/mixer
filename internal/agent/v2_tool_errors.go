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

// search_indicators: notes. These are returned in successful responses.
//
// Empty-result notes state only what was checked. They deliberately give no retry guidance: agents decide
// on their own whether to rephrase, and hints about where else data might be tend to send them looping.

// v2NoEntitiesNote is returned when no entity_dcids were given, so results were not filtered.
const v2NoEntitiesNote = "results are not filtered by data availability, which varies by entity; pass entity_dcids from resolve_entities to keep only indicators with data for those entities"

// v2NoMatchNote is returned when a search matched no topics or variables at all.
const v2NoMatchNote = "no topics or variables match this query."

// Empty-result notes for when entity_dcids filtered out every result.
const (
	// v2NoEntityDataNote: none of the entities has observation data for any variable.
	v2NoEntityDataNote = "there is no data for the given entity_dcids."
	// v2NoQueryDataNote: the search matched topics or variables, but none has data for the entities.
	v2NoQueryDataNote = "none of the topics or variables matching this query have data for the given entity_dcids."
	// v2NoRequestedTopicDataNote: none of the requested topics has data for the entities.
	v2NoRequestedTopicDataNote = "none of the requested topics have data for the given entity_dcids."
	// v2NoTopLevelTopicDataNote: none of the top-level topics has data for the entities.
	v2NoTopLevelTopicDataNote = "no topics have data for the given entity_dcids."
)

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
	st := status.FromContextError(err)
	if st.Code() == codes.Unknown {
		// errors.As finds the innermost status, so wrapped errors keep only the status message (status.FromError
		// would use the full wrapped text).
		var withStatus interface{ GRPCStatus() *status.Status }
		if errors.As(err, &withStatus) {
			st = withStatus.GRPCStatus()
		} else {
			st = status.New(codes.Internal, err.Error())
		}
	}
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
