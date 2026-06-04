// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

// ReadFile reads content from a local file or a GCS path (gs://...).
func ReadFile(ctx context.Context, filePath string) ([]byte, error) {
	if strings.HasPrefix(filePath, "gs://") {
		localPath := strings.TrimPrefix(filePath, "gs://")
		parts := strings.SplitN(localPath, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid gcs path: %s", filePath)
		}
		bucket := parts[0]
		object := parts[1]

		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage client: %w", err)
		}
		defer func() { _ = client.Close() }()

		rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read from gcs: %w", err)
		}
		defer func() { _ = rc.Close() }()

		return io.ReadAll(rc)
	}
	return os.ReadFile(filePath)
}
