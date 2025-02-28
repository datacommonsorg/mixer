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

package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRedisAddress(t *testing.T) {
	yamlConfig := `
instances:
  - region: us-central1
    host: redis-us-central1.example.com
    port: "6379"
  - region: us-east1
    host: redis-us-east1.example.com
    port: "6380"
`
	tests := []struct {
		name   string
		region string
		want   string
	}{
		{
			name:   "us-central1",
			region: "us-central1",
			want:   "redis-us-central1.example.com:6379",
		},
		{
			name:   "us-east1",
			region: "us-east1",
			want:   "redis-us-east1.example.com:6380",
		},
		{
			name:   "default",
			region: "",
			want:   "redis-us-central1.example.com:6379",
		},
		{
			name:   "unconfigured region",
			region: "europe-west1",
			want:   "redis-us-central1.example.com:6379",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address, err := getRedisAddress(yamlConfig, test.region)
			assert.NoError(t, err)
			assert.Equal(t, test.want, address)
		})
	}
}
