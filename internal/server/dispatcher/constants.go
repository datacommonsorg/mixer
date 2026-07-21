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

package dispatcher

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
)

// Custom unexported type for context keys prevents collisions across packages.
type contextKey string

// RelationExpressionExpandedEntities is the context key for expanded entities from pre-processed relation expressions.
const RelationExpressionExpandedEntities contextKey = "RelationExpressionExpandedEntities"

const sdmxContainedInPlaceToRemoteDCIDsKey contextKey = "SdmxContainedInPlaceToRemoteDCIDs"

// SdmxContainedInPlaceToRemoteDCIDs maps an SDMX containment relation to the
// remote child places resolved for it.
type SdmxContainedInPlaceToRemoteDCIDs map[datacommons.ContainedInPlaceConstraint][]string

// WithSdmxContainedInPlaceToRemoteDCIDs stores remote SDMX containment
// expansions in the request context.
func WithSdmxContainedInPlaceToRemoteDCIDs(
	ctx context.Context,
	containedInPlaceToRemoteDCIDs SdmxContainedInPlaceToRemoteDCIDs,
) context.Context {
	return context.WithValue(ctx, sdmxContainedInPlaceToRemoteDCIDsKey, containedInPlaceToRemoteDCIDs)
}

// SdmxContainedInPlaceToRemoteDCIDsFromContext returns remote SDMX
// containment expansions stored by RelationExpressionProcessor.
func SdmxContainedInPlaceToRemoteDCIDsFromContext(ctx context.Context) SdmxContainedInPlaceToRemoteDCIDs {
	containedInPlaceToRemoteDCIDs, _ := ctx.Value(sdmxContainedInPlaceToRemoteDCIDsKey).(SdmxContainedInPlaceToRemoteDCIDs)
	return containedInPlaceToRemoteDCIDs
}
