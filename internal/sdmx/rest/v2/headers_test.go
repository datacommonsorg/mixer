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

package restv2

import (
	"context"
	"testing"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

func TestShouldLogSDMX(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{
			name: "missing metadata",
			ctx:  context.Background(),
		},
		{
			name: "missing header",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs("other", "true")),
		},
		{
			name: "false",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "false")),
		},
		{
			name: "other value",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "TRUE")),
		},
		{
			name: "true",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "true")),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldLogSDMX(tt.ctx); got != tt.want {
				t.Errorf("ShouldLogSDMX() = %v, want %v", got, tt.want)
			}
		})
	}
}
