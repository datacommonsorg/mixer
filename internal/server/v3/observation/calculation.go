// Copyright 2025 Google LLC
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

package observation

import (
	"log"

	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
)

// CalculationProcessor implements the dispatcher.Processor interface for performing calculations.
type CalculationProcessor struct {
	// Set and use datasources if needed.
	_ *datasources.DataSources
}

func (processor *CalculationProcessor) PreProcess(requestContext *dispatcher.RequestContext) error {
	switch requestContext.Type {
	case dispatcher.TypeObservation:
		log.Printf("Pre-processing observation request.")
		return nil
	default:
		log.Printf("NOT pre-processing request of type: %s", requestContext.Type)
		return nil
	}
}

func (processor *CalculationProcessor) PostProcess(requestContext *dispatcher.RequestContext) error {
	switch requestContext.Type {
	case dispatcher.TypeObservation:
		log.Printf("Post-processing observation request.")
		return nil
	default:
		log.Printf("NOT post-processing request of type: %s", requestContext.Type)
		return nil
	}
}
