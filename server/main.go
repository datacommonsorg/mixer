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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/datacommonsorg/mixer/handler"
	"github.com/datacommonsorg/mixer/store"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"
	"github.com/gorilla/mux"
)

var (
	bqDataset  = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	btTable    = flag.String("bt_table", "", "DataCommons Bigtable table.")
	projectID  = flag.String("project_id", "", "The cloud project to run the mixer instance.")
	schemaPath = flag.String("schema_path", "/mixer/config/mapping", "Path to the schema mapping directory.")
	port       = flag.Int("port", 12345, "Port on which to run the server.")
)

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	subTypeMap, err := translator.GetSubTypeMap("translator/table_types.json")
	if err != nil {
		log.Fatalf("translator.GetSubTypeMap() = %v", err)
	}

	containedIn, err := util.GetContainedIn("type_relation.json")
	if err != nil {
		log.Fatalf("util.GetContainedIn() = %v", err)
	}

	st, err := store.NewStore(ctx, *bqDataset, *btTable, *projectID,
		*schemaPath, subTypeMap, containedIn)
	if err != nil {
		log.Fatalf("Failed to create store for %s, %s: %s", *bqDataset, *projectID, err)
	}

	r := mux.NewRouter()

	r.Path("/query").Methods("POST").Handler(handler.AppHandler{handler.QueryPostHandler, st})
	r.Path("/query").Methods("GET").Handler(handler.AppHandler{handler.QueryGetHandler, st})

	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
