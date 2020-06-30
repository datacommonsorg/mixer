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
	"flag"
	"log"

	"github.com/datacommonsorg/mixer/server"
)

var (
	bqDataset   = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	btTable     = flag.String("bt_table", "", "DataCommons Bigtable table.")
	btProject   = flag.String("bt_project", "", "GCP project containing the BigTable instance.")
	btInstance  = flag.String("bt_instance", "", "BigTable instance.")
	projectID   = flag.String("project_id", "", "The cloud project to run the mixer instance.")
	schemaPath  = flag.String("schema_path", "/mixer/config/mapping", "Path to the schema mapping directory.")
	port        = flag.String("port", ":12345", "Port on which to run the server.")
	branchCache = flag.Bool("branch_cache", true, "Whether to load branch cache")
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	s, err := server.NewServer(*port, *bqDataset, *btTable, *btProject, *btInstance,
		*projectID, *schemaPath, *branchCache)
	if err != nil {
		log.Fatalf("failed to create mixer: %v", err)
	}
	if err := s.Srv.Serve(s.Lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
