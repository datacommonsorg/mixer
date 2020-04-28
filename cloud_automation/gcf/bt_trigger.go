package btcachegeneration

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"time"

	"google.golang.org/api/dataflow/v1b3"
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
)

const (
	projectID          = "google.com:datcom-store-dev"
	bigtableInstance   = "prophet-cache"
	bigtableCluster    = "prophet-cache-c1"
	createTableRetries = 3
	columnFamily       = "csv"
	dataflowTemplate   = "gs://datcom-dataflow-templates/templates/csv_to_bt"
	bigtableNodes      = 300
)

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

func ReadFromGCS(ctx context.Context, bucketName, fileName string) ([]byte, error) {
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("Failed to create gcsClient: %v\n", err)
		return nil, fmt.Errorf("Failed to create gcsClient: %v", err)
	}

	bucket := gcsClient.Bucket(bucketName)
	rc, err := bucket.Object(fileName).NewReader(ctx)
	if err != nil {
		log.Printf("Unable to open file from bucket %q, file %q: %v\n", bucketName, fileName, err)
		return nil, fmt.Errorf("Unable to open file from bucket %q, file %q: %v", bucketName, fileName, err)
	}
	defer rc.Close()
	return ioutil.ReadAll(rc)
}

func SetupBigtable(ctx context.Context, tableID string) error {
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

	// Scale up bigtable cluster. This helps speed up the dataflow job.
	// We scale down again once dataflow job completes.
	instanceAdminClient, err := bigtable.NewInstanceAdminClient(ctx, projectID)
	if err != nil {
		log.Printf("Unable to create a table instance admin client. %v", err)
		return err
	}
	if err := instanceAdminClient.UpdateCluster(dctx, bigtableInstance, bigtableCluster, bigtableNodes); err != nil {
		log.Printf("Unable to increase bigtable cluster size: %v", err)
		return err
	}
	return nil
}

// GCSTrigger consumes a GCS event.
func GCSTrigger(ctx context.Context, e GCSEvent) error {
	return nil
	// Read contents of GCS file.
	inputFile, err := ReadFromGCS(ctx, e.Bucket, e.Name)
	if err != nil {
		log.Printf("Unable to read from gcs gs://%s/%s, got err: %v", e.Bucket, e.Name, err)
		return err
	}
	tableID := filepath.Base(filepath.Dir(string(inputFile)))
	SetupBigtable(ctx, tableID)

	// Call cloud dataflow template
	dataflowService, err := dataflow.NewService(ctx)
	if err != nil {
		log.Printf("Unable to create dataflow client: borgcron_2020_04_10_02_32_53. %v", err)
		return err
	}
	prjSrv := dataflow.NewProjectsTemplatesService(dataflowService)
	jobParams := map[string]string{
		"bigtableInstanceId": bigtableInstance,
		"bigtableTableId":    string(tableID),
		"inputFile":          string(inputFile),
		"bigtableProjectId":  projectID,
	}

	tmplParams := &dataflow.LaunchTemplateParameters{
		JobName:    tableID,
		Parameters: jobParams,
		Environment: &dataflow.RuntimeEnvironment{
			IpConfiguration: "WORKER_IP_PRIVATE",
			//NumWorkers:      600,
			WorkerRegion:    "us-central1",
		},
	}
	c := prjSrv.Launch(projectID, tmplParams)
	c.GcsPath(dataflowTemplate)
	_, err = c.Do()
	if err != nil {
		log.Printf("Template launch failed: %v", err)
	}
	return nil
}

