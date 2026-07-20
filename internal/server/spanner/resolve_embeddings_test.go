package spanner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockEmbedContentTransport struct {
	embeddings []float64
}

func (m *mockEmbedContentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	values := m.embeddings
	if values == nil {
		values = []float64{}
	}
	respJSON, _ := json.Marshal(map[string]any{
		"predictions": []map[string]any{
			{
				"embeddings": map[string]any{
					"values": values,
				},
			},
		},
		"embedding": map[string]any{
			"values": values,
		},
		"embeddings": []map[string]any{
			{"values": values},
		},
	})
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(respJSON)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

func newMockGenAIClient(embeddings []float64) *genai.Client {
	httpClient := &http.Client{
		Transport: &mockEmbedContentTransport{embeddings: embeddings},
	}
	client, _ := genai.NewClient(context.Background(), &genai.ClientConfig{
		Project:    "test-project",
		Location:   "us-central1",
		HTTPClient: httpClient,
		Backend:    genai.BackendVertexAI,
	})
	return client
}

func TestResolveEmbeddingsEmpty(t *testing.T) {
	t.Parallel()

	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		embeddingsRes: []float64{},
	}, nil)
	ds.genaiClient = newMockGenAIClient([]float64{})

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"California"},
		Resolver: "indicator",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node:       "California",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveEmbeddingsSuccess(t *testing.T) {
	t.Parallel()

	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		embeddingsRes: []float64{0.1, 0.2},
		vectorSearchRes: []*VectorSearchResult{
			{
				SubjectID:        "dc/topic/Climate",
				Name:             "Climate Change",
				CosineSimilarity: 0.85,
				Types:            []string{"Topic"},
			},
		},
	}, nil)
	ds.genaiClient = newMockGenAIClient([]float64{0.1, 0.2})

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"Climate"},
		Resolver: "indicator",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "Climate",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid:   "dc/topic/Climate",
						TypeOf: []string{"Topic"},
						Metadata: map[string]string{
							"score":    "0.8500",
							"sentence": "Climate Change",
						},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveEmbeddingsConcurrentSuccess(t *testing.T) {
	t.Parallel()

	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		embeddingsRes: []float64{0.1, 0.2},
		vectorSearchRes: []*VectorSearchResult{
			{
				SubjectID:        "dc/topic/Climate",
				Name:             "Climate Change",
				CosineSimilarity: 0.85,
				Types:            []string{"Topic"},
			},
		},
	}, nil)
	ds.genaiClient = newMockGenAIClient([]float64{0.1, 0.2})


	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"Climate", "Environment", "Weather"},
		Resolver: "indicator",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "Climate",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid:   "dc/topic/Climate",
						TypeOf: []string{"Topic"},
						Metadata: map[string]string{
							"score":    "0.8500",
							"sentence": "Climate Change",
						},
					},
				},
			},
			{
				Node: "Environment",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid:   "dc/topic/Climate",
						TypeOf: []string{"Topic"},
						Metadata: map[string]string{
							"score":    "0.8500",
							"sentence": "Climate Change",
						},
					},
				},
			},
			{
				Node: "Weather",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid:   "dc/topic/Climate",
						TypeOf: []string{"Topic"},
						Metadata: map[string]string{
							"score":    "0.8500",
							"sentence": "Climate Change",
						},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveTopic_NoExpander(t *testing.T) {
	t.Parallel()

	ds := NewSpannerDataSource(nil, nil)

	_, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"dc/topic/Climate"},
		Resolver: "topic",
	})
	if err == nil {
		t.Fatalf("Resolve() expected error, got nil")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Resolve() expected FailedPrecondition error, got: %v", err)
	}
}

type mockTopicExpander struct {
	resolve.TopicExpander
	expandTopicRes []*pbv2.ResolveResponse_Entity_Candidate
	expandTopicErr error
	displayNameRes string
}

func (m *mockTopicExpander) ExpandTopic(ctx context.Context, topicDcid string, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	return m.expandTopicRes, m.expandTopicErr
}

func (m *mockTopicExpander) GetTopicDisplayName(ctx context.Context, topicDcid string) string {
	return m.displayNameRes
}

func TestResolveTopic_Success(t *testing.T) {
	t.Parallel()

	mockExpander := &mockTopicExpander{
		displayNameRes: "Climate",
		expandTopicRes: []*pbv2.ResolveResponse_Entity_Candidate{
			{
				Dcid:   "dc/topic/ClimateChange",
				TypeOf: []string{"Topic"},
			},
		},
	}

	ds := NewSpannerDataSource(nil, &SpannerDataSourceOptions{
		TopicExpander: mockExpander,
	})

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"dc/topic/Climate"},
		Resolver: "topic",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "dc/topic/Climate",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid:   "dc/topic/Climate",
						TypeOf: []string{"Topic"},
						Name:   "Climate",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "dc/topic/ClimateChange",
								TypeOf: []string{"Topic"},
							},
						},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}
