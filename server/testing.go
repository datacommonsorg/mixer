// Copyright 2020 Google LLC
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

package server

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

func setupBigtable(
	ctx context.Context, data map[string]string) (*bigtable.Table, error) {
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

	if err := adminClient.CreateColumnFamily(
		ctx, testTable, util.BtFamily); err != nil {
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
	return client.Open(testTable), err
}
