package e2etest

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
)

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	btProject   = "google.com:datcom-store-dev"
	btInstance  = "prophet-cache"
	bqProject   = "datcom-ci"
	branchCache = false
)

func Setup(ctx context.Context) (pb.MixerClient, error) {
	_, filename, _, _ := runtime.Caller(0)
	btTable, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bt_table.txt"))
	bqTable, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bq_table.txt"))
	// Use a fixed BQ dataset for now.
	schemaPath := path.Join(path.Dir(filename), "../")

	s, err := server.NewServer(
		"localhost:0",
		strings.TrimSpace(string(bqTable)),
		strings.TrimSpace(string(btTable)),
		btProject,
		btInstance,
		bqProject,
		schemaPath,
		branchCache,
	)
	if err != nil {
		return nil, err
	}
	// Start mixer at localhost:0
	go s.Srv.Serve(s.Lis)

	// Create mixer client
	conn, err := grpc.Dial(
		s.Addr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)))
	if err != nil {
		return nil, err
	}
	client := pb.NewMixerClient(conn)
	return client, nil
}

func TestResponse(t *testing.T) {
	ctx := context.Background()
	client, err := Setup(ctx)
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_stats")
	resp, err := client.GetStats(ctx, &pb.GetStatsRequest{
		StatsVar: "TotalPopulation",
		Place:    []string{"geoId/06"},
	})
	if err != nil {
		t.Fatalf("could not GetStats: %s", err)
	}
	var result map[string]*pb.ObsTimeSeries
	err = json.Unmarshal([]byte(resp.GetPayload()), &result)
	if err != nil {
		t.Errorf("Can not Unmarshal payload")
	}

	var expected map[string]*pb.ObsTimeSeries
	file, _ := ioutil.ReadFile(path.Join(goldenPath, "California_TotalPopulation.json"))
	err = json.Unmarshal(file, &expected)
	if err != nil {
		t.Errorf("Can not Unmarshal golden file")
	}

	if diff := cmp.Diff(result, expected); diff != "" {
		t.Errorf("payload got diff: %v", diff)
	}
}
