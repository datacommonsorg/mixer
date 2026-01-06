package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func TestV2ResolveCore_EmbeddingsFlag(t *testing.T) {
	ctx := context.Background()

	// 1. Test with flag DISABLED
	sDisabled := &Server{
		flags: &featureflags.Flags{
			EnableEmbeddingsResolver: false,
		},
		httpClient: &http.Client{},
	}
	req := &pbv2.ResolveRequest{
		Resolver: "indicator",
		Nodes:    []string{"foo"},
	}
	_, err := sDisabled.V2ResolveCore(ctx, req)
	if err == nil {
		t.Error("Expected error when flag is disabled, got nil")
	} else {
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("Expected Unimplemented error, got %v", err)
		}
	}

	// 2. Test with flag ENABLED
	// We mock the HTTP client to avoid real network calls, confirming it reached the logic
	// inside ResolveEmbeddings (which uses the client).
	called := false
	mockClient := &http.Client{
		Transport: &mockTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				called = true
				// Return a fake error to stop execution there, safely proving we passed the check
				return nil, context.Canceled
			},
		},
	}

	sEnabled := &Server{
		flags: &featureflags.Flags{
			EnableEmbeddingsResolver: true,
		},
		httpClient: mockClient,
	}

	_, _ = sEnabled.V2ResolveCore(ctx, req)

	if !called {
		t.Error("Expected HTTP client to be called when flag is enabled")
	}
}
