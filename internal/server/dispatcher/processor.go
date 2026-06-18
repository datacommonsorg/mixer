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

// Package dispatcher defines the core Dispatcher framework and the Processor interface.
// Processors act as middleware in the request fulfillment pipeline, allowing for
// pre-processing (e.g., caching, request augmentation) and post-processing
// (e.g., response synthesis, formula evaluation) of requests.
package dispatcher

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// RequestType represents the type of request.
type RequestType string

const (
	TypeNode                   RequestType = "Node"
	TypeNodeSearch             RequestType = "NodeSearch"
	TypeObservation            RequestType = "Observation"
	TypeResolve                RequestType = "Resolve"
	TypeSparql                 RequestType = "Sparql"
	TypeEvent                  RequestType = "Event"
	TypeBulkVariableInfo       RequestType = "BulkVariableInfo"
	TypeBulkVariableGroupInfo  RequestType = "BulkVariableGroupInfo"
	TypeSdmxData               RequestType = "SdmxData"
	TypeSdmxAvailability       RequestType = "SdmxAvailability"
	TypeFilterStatVarsByEntity RequestType = "FilterStatVarsByEntity"
)

// RequestContext holds state for a single request as it flows through processors.
type RequestContext struct {
	context.Context
	Type            RequestType
	OriginalRequest proto.Message
	CurrentRequest  proto.Message
	CurrentResponse proto.Message
}

// Outcome represents the result of a processing step.
type Outcome int

const (
	// Continue indicates that processing should continue.
	// This should be the default outcome of most processing steps.
	Continue Outcome = iota
	// Done indicates that processing should stop.
	// With this outcome, the current response is returned immediately.
	Done
)

// Processor interface defines methods for performing pre and post processing operations.
type Processor interface {
	PreProcess(*RequestContext) (Outcome, error)
	PostProcess(*RequestContext) (Outcome, error)
}
