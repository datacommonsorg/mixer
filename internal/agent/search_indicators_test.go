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

package agent

import (
	"context"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// failingResolveMixer fails V2Resolve calls for one resolver and delegates everything else to mockMixerServer.
type failingResolveMixer struct {
	*mockMixerServer
	failResolver string
	err          error
}

func (m *failingResolveMixer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	if in.GetResolver() == m.failResolver {
		return nil, m.err
	}
	return m.mockMixerServer.V2Resolve(ctx, in)
}

func TestSharedSearchHelpersReturnBackendErrors(t *testing.T) {
	backendErr := status.Error(codes.FailedPrecondition, "resolution unavailable")
	for _, tc := range []struct {
		name         string
		failResolver string
		call         func(*Service) error
	}{
		{
			name:         "fetchCandidates",
			failResolver: ResolverIndicator,
			call: func(s *Service) error {
				_, err := s.fetchCandidates(context.Background(), "health", 0, false, "")
				return err
			},
		},
		{
			name:         "fetchDescendantVariables",
			failResolver: ResolverTopic,
			call: func(s *Service) error {
				_, err := s.fetchDescendantVariables(context.Background(), []string{"dc/topic/Health"}, "")
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &failingResolveMixer{mockMixerServer: &mockMixerServer{}, failResolver: tc.failResolver, err: backendErr}
			err := tc.call(NewService(mock, NewCache(mock), nil))
			if err != backendErr {
				t.Errorf("%s() error = %v, want the backend error unchanged: %v", tc.name, err, backendErr)
			}
		})
	}
}
