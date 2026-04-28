package spanner

import (
	"context"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestResolveEmbeddingsEmpty(t *testing.T) {
	t.Parallel()

	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		embeddingsRes: []float64{},
	}, nil, nil)

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
	}, nil, nil)

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
