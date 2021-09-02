// Copyright 2019 Google LLC
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

package store

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func LoadTmcfCsv(ctx context.Context, client *mongo.Client) error {
	collection := client.Database("testing").Collection("observations")
	mongoCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := collection.InsertOne(mongoCtx, bson.D{primitive.E{Key: "foo", Value: "bar"}})
	if err != nil {
		return err
	}
	log.Println(res.InsertedID)
	return nil
}
