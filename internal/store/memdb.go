// Copyright 2019 Google LLC
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

package store

import (
	"io/ioutil"
	"log"

	"github.com/datacommonsorg/mixer/internal/parser"
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// MemDb holds imported data in memory.
type MemDb struct {
	statSeries map[string]map[string]pb.SeriesMap
}

func (memDb *MemDb) Load(tmcfFile string) error {
	tmcfBytes, err := ioutil.ReadFile(tmcfFile)
	if err != nil {
		return err
	}
	schemaMapping, err := parser.ParseTmcf(string(tmcfBytes))
	// TODO: implement csv parsing and actual data loading
	log.Printf("%v", schemaMapping)
	if err != nil {
		return err
	}
	memDb.statSeries = map[string]map[string]pb.SeriesMap{}
	return nil
}
