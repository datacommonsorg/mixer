// Copyright 2023 Google LLC
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

package data

import (
	"context"
	"database/sql"
	"path/filepath"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/sqlite/writer"
	"github.com/datacommonsorg/mixer/internal/store"
)

// Import implements API for Mixer.Import.
func Import(
	ctx context.Context,
	in *pb.ImportRequest,
	st *store.Store,
	metadata *resource.Metadata,
	openSql bool,
) (*resource.Cache, error) {
	var err error
	if err = writer.Write(
		metadata,
	); err != nil {
		return nil, err
	}
	var c *resource.Cache
	if openSql {
		if st.SQLiteClient != nil {
			st.SQLiteClient.Close()
		}
		sqlClient, err := sql.Open(
			"sqlite3", filepath.Join(metadata.SQLitePath, "datacommons.db"))
		if err != nil {
			return nil, err
		}
		st.SQLiteClient = sqlClient
		c, err = cache.NewCache(
			ctx,
			st,
			cache.SearchOptions{
				UseSearch:           true,
				BuildSvgSearchIndex: true,
			},
		)
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}
