package store

import (
	"context"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testProject  = "project"
	testInstance = "instance"
	testTable    = "dc"
)

// NewTestBtStore creates a new store for testing BigTable.
func NewTestBtStore(ctx context.Context, data map[string]string) (Interface, error) {
	btClient, err := SetupBigtable(ctx, data)
	if err != nil {
		return nil, err
	}
	return &store{"", nil, nil, nil, nil, nil, nil, btClient.Open(testTable), nil}, nil
}

func SetupBigtable(ctx context.Context, data map[string]string) (*bigtable.Client, error) {
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	adminClient, err := bigtable.NewAdminClient(ctx, testProject, testInstance,
		option.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	if err := adminClient.CreateTable(ctx, testTable); err != nil {
		return nil, err
	}

	if err := adminClient.CreateColumnFamily(ctx, testTable, util.BtFamily); err != nil {
		return nil, err
	}

	client, err := bigtable.NewClient(ctx, testProject, testInstance,
		option.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}
	bt := client.Open(testTable)

	mut := bigtable.NewMutation()
	for key, value := range data {
		mut.Set(util.BtFamily, "value", bigtable.Now(), []byte(value))
		if err = bt.Apply(ctx, key, mut); err != nil {
			return nil, err
		}
	}
	return client, err
}

func TestReadRowsParallel(t *testing.T) {
	ctx := context.Background()
	data := map[string]string{
		"key1": "data1",
		"key2": "data2",
	}
	btClient, err := SetupBigtable(ctx, data)
	if err != nil {
		t.Errorf("setupBigtable got error: %v", err)
	}
	btTable := btClient.Open(testTable)
	results := map[string]string{}
	rowList := bigtable.RowList{"key1", "key2"}
	if err := bigTableReadRowsParallel(ctx, btTable, rowList,
		func(btRow bigtable.Row) error {
			results[btRow.Key()] = string(btRow[util.BtFamily][0].Value)
			return nil
		}); err != nil {
		t.Errorf("btReadRowsParallel got error: %v", err)
	}
	if diff := cmp.Diff(data, results); diff != "" {
		t.Errorf("read rows got diff from table data %+v", diff)
	}
}

func TestIsterateSortPVs(t *testing.T) {
	var pvs = []*pb.PropertyValue{
		{
			Property: "gender",
			Value:    "Male",
		},
		{
			Property: "age",
			Value:    "Years85Onwards",
		},
	}
	got := "^populationType"
	if len(pvs) > 0 {
		iterateSortPVs(pvs, func(i int, p, v string) {
			got += ("^" + p + "^" + v)
		})
	}

	want := "^populationType^age^Years85Onwards^gender^Male"
	if got != want {
		t.Errorf("iterateSortPVs() = %s, want %s", got, want)
	}
}
