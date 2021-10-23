// Copyright 2021 Google LLC
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

package pubsub

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/datacommonsorg/mixer/internal/util"
)

// Subscribe does the following:
// 1) Create a subscriber within the instance.
// 2) Start a receiver in a goroutine.
// 3) Have a goroutine to delete the subscriber on server shutdown.
//
// TODO(shifucun): Add unittest.
func Subscribe(
	ctx context.Context,
	project string,
	prefix string,
	topic string,
	worker func(ctx context.Context, msg *pubsub.Message) error,
) error {
	// Create pubsub client.
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return err
	}
	// Always create a new subscriber with default expiration date of 2 days.
	subID := prefix + util.RandomString()
	expiration, _ := time.ParseDuration("36h")
	retention, _ := time.ParseDuration("24h")
	subscriber, err := client.CreateSubscription(ctx, subID,
		pubsub.SubscriptionConfig{
			Topic:             client.Topic(topic),
			ExpirationPolicy:  expiration,
			RetentionDuration: retention,
		})
	if err != nil {
		return err
	}
	log.Printf("Created subscriber with id: %s\n", subID)
	// Start the receiver in a goroutine.
	go func() {
		err = subscriber.Receive(
			ctx,
			func(ctx context.Context, msg *pubsub.Message) {
				msg.Ack()
				err = worker(ctx, msg)
				if err != nil {
					log.Printf("Subscriber can not complete task: %v", err)
				}
			})
		if err != nil {
			log.Printf("Cloud pubsub receive: %v", err)
		}
	}()
	// Create a go routine to check server shutdown and delete the subscriber.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		err := subscriber.Delete(ctx)
		if err != nil {
			log.Fatalf("Failed to delete subscriber: %v", err)
		}
		log.Printf("Deleted subscriber: %v", subscriber)
		os.Exit(1)
	}()
	return nil
}
