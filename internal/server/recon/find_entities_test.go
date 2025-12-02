package recon

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFindEntitiesEmptyDescription(t *testing.T) {
	// Create a dummy store to avoid nil pointer dereference
	s := &store.Store{
		RecogPlaceStore: &files.RecogPlaceStore{},
	}

	ctx := context.Background()
	req := &pb.FindEntitiesRequest{
		Description: "", // Empty description
	}

	resp, err := FindEntities(ctx, req, s, nil)
	if err != nil {
		t.Fatalf("FindEntities returned error: %v", err)
	}

	if resp == nil {
		t.Fatal("FindEntities returned nil response")
	}

	if len(resp.GetDcids()) != 0 {
		t.Errorf("Expected empty DCIDs, got: %v", resp.GetDcids())
	}

	// Ensure it matches expected empty proto
	want := &pb.FindEntitiesResponse{}
	if diff := cmp.Diff(resp, want, protocmp.Transform()); diff != "" {
		t.Errorf("FindEntities() mismatch (-want +got):\n%s", diff)
	}
}
