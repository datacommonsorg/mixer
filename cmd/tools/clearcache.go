// Copyright 2025 Google LLC
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

// A tool to flush (clear) the redis cache.
// This tool is intended to be run on a kubernetes container.

// Usage on kubernetes:
// /go/bin/tools/clearcache --redis_host=REDIS_HOST [--redis_port=REDIS_PORT]
// Usage on local:
// go run cmd/tools/clearcache.go --redis_host=REDIS_HOST [--redis_port=REDIS_PORT]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
)

var (
	redisHost = flag.String("redis_host", "", "The redis host.")
	redisPort = flag.String("redis_port", "6379", "The redis port.")
)

func main() {
	flag.Parse()

	if *redisHost == "" {
		fmt.Println("Error: Redis host not specified.")
		os.Exit(1)
	}

	ctx := context.Background()
	redisAddr := fmt.Sprintf("%s:%s", *redisHost, *redisPort)
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	err := rdb.FlushAll(ctx).Err()
	if err != nil {
		fmt.Printf("Error clearing redis cache: %s\n", redisAddr)
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cleared redis cache: %s\n", redisAddr)
}
