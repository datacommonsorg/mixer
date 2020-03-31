package store

import (
	"context"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testProject  = "project"
	testInstance = "instance"
	testTable    = "dc"
)

func newTestBtStore(ctx context.Context, opts ...option.ClientOption) (Interface, error) {
	btClient, err := bigtable.NewClient(ctx, testProject, testInstance, opts...)
	if err != nil {
		return nil, err
	}

	return &store{
		"", nil, nil, nil, nil, nil, nil, btClient.Open(testTable), nil}, nil
}

func setupBigtable(ctx context.Context, data map[string]string) (*grpc.ClientConn, error) {
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

	return conn, err
}
