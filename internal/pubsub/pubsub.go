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
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/types/known/durationpb"
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
	subName := "projects/" + project + "/subscriptions/" + prefix + util.RandomString()
	topicName := "projects/" + project + "/topics/" + topic
	expiration := &durationpb.Duration{Seconds: int64(36 * 60 * 60)} // 36 hours
	retention := &durationpb.Duration{Seconds: int64(24 * 60 * 60)}  // 24 hours
	subscription, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                     subName,
		Topic:                    topicName,
		ExpirationPolicy:         &pubsubpb.ExpirationPolicy{Ttl: expiration},
		MessageRetentionDuration: retention,
	})
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}
	subscriber := client.Subscriber(subscription.GetName())
	slog.Info("Created subscriber", "name", subName, "topic", topicName)
	// Start the receiver in a goroutine.
	go func() {
		err = subscriber.Receive(
			ctx,
			func(ctx context.Context, msg *pubsub.Message) {
				msg.Ack()
				err = worker(ctx, msg)
				if err != nil {
					slog.Error("Subscriber can not complete task", "error", err)
				}
			})
		if err != nil {
			slog.Error("Cloud pubsub receive", "error", err)
		}
	}()
	// Create a go routine to check server shutdown and delete the subscriber.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{
			Subscription: subscription.GetName(),
		})
		if err != nil {
			slog.Error("Failed to delete subscriber", "error", err)
			os.Exit(1)
		}
		slog.Info("Deleted subscriber", "subscriber", subscriber)
		os.Exit(1)
	}()
	return nil
}
