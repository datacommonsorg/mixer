// Package btcachegeneration runs a GCF function that triggers in 2 scenarios:
// 1) completion of prophet-flume job in borg.
//    The trigger is based on GCS file prophet-cache/latest_base_cache_run.txt.
// 2) On completion of BT cache ingestion via an airflow job. This trigger is based
//    on GCS file prophet-cache/[success|failure].txt
//
// In the first case, on triggering it sets up new cloud BT table, scales up BT cluster to 300 nodes
// and starts an airflow job by writing to prophet-cache/airflow.txt
//
// In the second case it scales BT cluster to 20 nodes.
package btcachegeneration

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	projectID          = "google.com:datcom-store-dev"
	bigtableInstance   = "prophet-cache"
	bigtableCluster    = "prophet-cache-c1"
	createTableRetries = 3
	columnFamily       = "csv"
	bigtableNodesHigh  = 300
	bigtableNodesLow   = 20
	triggerFile        = "latest_base_cache_version.txt"
	successFile        = "success.txt"
	failureFile        = "failure.txt"
	airflowTriggerFile = "airflow_trigger.txt"
)

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

func readFromGCS(ctx context.Context, gcsClient *storage.Client, bucketName, fileName string) ([]byte, error) {
	bucket := gcsClient.Bucket(bucketName)
	rc, err := bucket.Object(fileName).NewReader(ctx)
	if err != nil {
		log.Printf("Unable to open file from bucket %q, file %q: %v\n", bucketName, fileName, err)
		return nil, status.Errorf(
			codes.Internal, "Unable to open file from bucket %q, file %q: %v", bucketName, fileName, err)
	}
	defer rc.Close()
	return ioutil.ReadAll(rc)
}

func writeToGCS(ctx context.Context, gcsClient *storage.Client, bucketName, fileName string, data string) error {
	bucket := gcsClient.Bucket(bucketName)
	w := bucket.Object(fileName).NewWriter(ctx)

	if _, err := fmt.Fprint(w, data); err != nil {
		w.Close()
		log.Printf("Unable to open file for writing from bucket %q, file %q: %v\n", bucketName, fileName, err)
		return status.Errorf(codes.Internal, "Unable to write to bucket %q, file %q: %v", bucketName, fileName, err)
	}
	return w.Close()
}

func setupBigtable(ctx context.Context, tableID string) error {
	log.Printf("Creating new bigtable table: %s", tableID)
	adminClient, err := bigtable.NewAdminClient(ctx, projectID, bigtableInstance)
	if err != nil {
		log.Printf("Unable to create a table admin client. %v", err)
		return err
	}

	// Create table. We retry 3 times in 1 minute intervals.
	dctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Minute))
	defer cancel()
	var ok bool
	for ii := 0; ii < createTableRetries; ii++ {
		if err = adminClient.CreateTable(dctx, tableID); err == nil {
			ok = true
			break
		}
		time.Sleep(1 * time.Minute)
	}
	if !ok {
		log.Printf("Unable to create table: %s, got error: %v", tableID, err)
		return err
	}

	// Create table columnFamily.
	if err := adminClient.CreateColumnFamily(dctx, tableID, columnFamily); err != nil {
		log.Printf("Unable to create column family: csv for table: %s, got error: %v", tableID, err)
		return err
	}
	return scaleBT(ctx, bigtableNodesHigh)
}

func scaleBT(ctx context.Context, numNodes int32) error {
	// Scale up bigtable cluster. This helps speed up the dataflow job.
	// We scale down again once dataflow job completes.
	instanceAdminClient, err := bigtable.NewInstanceAdminClient(ctx, projectID)
	dctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Minute))
	defer cancel()
	if err != nil {
		log.Printf("Unable to create a table instance admin client. %v", err)
		return err
	}
	if err := instanceAdminClient.UpdateCluster(dctx, bigtableInstance, bigtableCluster, numNodes); err != nil {
		log.Printf("Unable to increase bigtable cluster size: %v", err)
		return err
	}
	return nil
}

// GCSTrigger consumes a GCS event.
func GCSTrigger(ctx context.Context, e GCSEvent) error {

	if strings.HasSuffix(e.Name, triggerFile) {
		// Read contents of GCS file. it contains path to csv files
		// for base cache.
		gcsClient, err := storage.NewClient(ctx)
		if err != nil {
			log.Printf("Failed to create gcsClient: %v\n", err)
			return status.Errorf(codes.Internal, "Failed to create gcsClient: %v", err)
		}

		tableID, err := readFromGCS(ctx, gcsClient, e.Bucket, e.Name)
		if err != nil {
			log.Printf("Unable to read from gcs gs://%s/%s, got err: %v", e.Bucket, e.Name, err)
			return err
		}
		// Create and scale up cloud BT.
		tableIDStr := strings.TrimSpace(fmt.Sprintf("%s", tableID))
		if err := setupBigtable(ctx, tableIDStr); err != nil {
			return nil
		}
		// Write to GCS file that triggers airflow job.
		inputFile := fmt.Sprintf("gs://prophet_cache/%s/cache.csv*", tableIDStr)
		err = writeToGCS(ctx, gcsClient, e.Bucket, airflowTriggerFile, inputFile)
		if err != nil {
			return nil
		}
	} else if strings.HasSuffix(e.Name, successFile) || strings.HasSuffix(e.Name, failureFile) {
		return scaleBT(ctx, bigtableNodesLow)
	}
	return nil
}
