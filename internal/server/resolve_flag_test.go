package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

// TestV2ResolveCore_EmbeddingsFlag verifies that the EnableEmbeddingsResolver feature flag
// correctly gates the embeddings resolution logic in V2ResolveCore.
// It ensures that:
// 1. When Disabled: The endpoint returns Unimplemented.
// 2. When Enabled: The endpoint proceeds to use the HTTP client (mocked here).
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
		Property: "<-description->dcid", // Must be valid for indicator resolver
		Nodes:    []string{"foo"},
	}

	_, err := sDisabled.V2ResolveCore(
		ctx,
		&resolve.NormalizedResolveRequest{
			Request:      req,
			InProp:       "description",
			OutProp:      "dcid",
			TypeOfValues: nil,
		})
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
		embeddingsServiceClient: resolve.NewEmbeddingsServiceClient(mockClient, "http://example.com", ""),
	}

	_, _ = sEnabled.V2ResolveCore(
		ctx,
		&resolve.NormalizedResolveRequest{
			Request:      req,
			InProp:       "description",
			OutProp:      "dcid",
			TypeOfValues: nil,
		})

	if !called {
		t.Error("Expected HTTP client to be called when flag is enabled")
	}
}

// Test: Server with nil embeddingsServiceClient.
// Situation: resolver is "indicator" and EnableEmbeddingsResolver is enabled, but embeddingsServiceClient is nil.
// Expectation: Returns FailedPrecondition error.
func TestV2ResolveCore_NilEmbeddingsClient(t *testing.T) {
	ctx := context.Background()

	s := &Server{
		flags: &featureflags.Flags{
			EnableEmbeddingsResolver: true,
		},
		embeddingsServiceClient: nil,
	}

	req := &pbv2.ResolveRequest{
		Resolver: "indicator",
		Property: "<-description->dcid",
		Nodes:    []string{"foo"},
	}

	_, err := s.V2ResolveCore(
		ctx,
		&resolve.NormalizedResolveRequest{
			Request:      req,
			InProp:       "description",
			OutProp:      "dcid",
			TypeOfValues: nil,
		})

	if err == nil {
		t.Error("Expected error when embeddingsServiceClient is nil, got nil")
	} else {
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("Expected FailedPrecondition error, got %v", err)
		}
	}
}

// Test: Server with invalid embeddings index.
// Situation: resolver is "indicator" and EnableEmbeddingsResolver is enabled.
//            The default index is configured as "invalid_idx", but the embeddings server config
//            only lists "base_uae_mem".
// Expectation: Once loaded, requests fail with InvalidArgument.
func TestV2ResolveCore_InvalidIndex(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"indexes": {"base_uae_mem": {}}}`))
	}))
	defer server.Close()

	client := resolve.NewEmbeddingsServiceClient(server.Client(), server.URL, "invalid_idx")

	s := &Server{
		flags: &featureflags.Flags{
			EnableEmbeddingsResolver: true,
		},
		embeddingsServiceClient: client,
	}

	req := &pbv2.ResolveRequest{
		Resolver: "indicator",
		Property: "<-description->dcid",
		Nodes:    []string{"foo"},
	}

	// 1. First call triggers the async load, should fail open (or not fail with InvalidArgument)
	_, err := s.V2ResolveCore(
		ctx,
		&resolve.NormalizedResolveRequest{
			Request:      req,
			InProp:       "description",
			OutProp:      "dcid",
			TypeOfValues: nil,
		})
	if err != nil && status.Code(err) == codes.InvalidArgument {
		t.Errorf("First call expected to fail open, but got InvalidArgument: %v", err)
	}

	// Wait for async load to finish
	time.Sleep(100 * time.Millisecond)

	// 2. Second call should fail with InvalidArgument
	_, err = s.V2ResolveCore(
		ctx,
		&resolve.NormalizedResolveRequest{
			Request:      req,
			InProp:       "description",
			OutProp:      "dcid",
			TypeOfValues: nil,
		})

	if err == nil {
		t.Error("Expected error for invalid index, got nil")
	} else {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Expected InvalidArgument error, got %v", err)
		}
		expectedMsg := `Embeddings index "invalid_idx" is not available in the embeddings server`
		if !strings.Contains(err.Error(), expectedMsg) {
			t.Errorf("Expected error to contain %q, got %q", expectedMsg, err.Error())
		}
	}
}

