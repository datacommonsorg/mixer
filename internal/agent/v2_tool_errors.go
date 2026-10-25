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

// wrapV2BackendError adds caller-facing context to a backend error while keeping its gRPC status code
// (e.g. InvalidArgument, Unimplemented, DeadlineExceeded), defaulting to Internal for other errors.
// Only the status message is kept so the caller-facing text is not cluttered with "rpc error: ..." prefixes.
func wrapV2BackendError(err error, msg string) error {
	st := status.FromContextError(err)
	if st.Code() == codes.Unknown {
		if parsed, ok := status.FromError(err); ok {
			st = parsed
		} else {
			st = status.New(codes.Internal, err.Error())
		}
	}
	return status.Errorf(st.Code(), "%s: %s", msg, st.Message())
}
