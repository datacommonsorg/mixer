// Copyright 2022 Google LLC
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

package bigtable

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

const (
	// BranchBigtableProject is the branch bigtable project
	BranchBigtableProject = "datcom-store"
	// BranchBigtableProject is the branch bigtable instance
	BranchBigtableInstance = "prophet-branch-cache"
	// BranchBigtableProject is the branch cache subscriber prefix
	BranchCacheSubscriberPrefix = "branch-cache-subscriber-"
	// BranchBigtableProject is the branch cache pubsub topic
	BranchCachePubsubTopic = "proto-branch-cache-reload"

	branchCacheVersionBucket = "datcom-control"
	branchCacheVersionFile   = "latest_proto_branch_cache_version.txt"
)

// ReadBranchTableName reads branch cache folder from GCS.
func ReadBranchTableName(ctx context.Context) (string, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	rc, err := client.Bucket(branchCacheVersionBucket).Object(branchCacheVersionFile).NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	folder, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(folder), nil
}
