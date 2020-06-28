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

// Package store is a library for querying datacommons backend storage.
package store

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"

	"github.com/datacommonsorg/mixer/base"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	gcsBucket     = "prophet_cache"
	subIDPrefix   = "mixer-subscriber-"
	pubsubTopic   = "branch-cache-reload"
	pubsubProject = "google.com:datcom-store-dev"
	versionFile   = "latest_branch_cache_version.txt"
)

// Interface exposes the database access for mixer.
type Interface interface {
	Query(ctx context.Context,
		in *pb.QueryRequest, out *pb.QueryResponse) error

	Search(ctx context.Context, in *pb.SearchRequest, out *pb.SearchResponse) error

	GetPropertyLabels(ctx context.Context,
		in *pb.GetPropertyLabelsRequest, out *pb.GetPropertyLabelsResponse) error

	GetPropertyValues(ctx context.Context,
		in *pb.GetPropertyValuesRequest, out *pb.GetPropertyValuesResponse) error

	GetTriples(ctx context.Context,
		in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error

	GetPopObs(ctx context.Context,
		in *pb.GetPopObsRequest, out *pb.GetPopObsResponse) error

	GetPlaceObs(ctx context.Context,
		in *pb.GetPlaceObsRequest, out *pb.GetPlaceObsResponse) error

	GetObsSeries(ctx context.Context,
		in *pb.GetObsSeriesRequest, out *pb.GetObsSeriesResponse) error

	GetPopCategory(ctx context.Context,
		in *pb.GetPopCategoryRequest, out *pb.GetPopCategoryResponse) error

	GetPopulations(ctx context.Context,
		in *pb.GetPopulationsRequest, out *pb.GetPopulationsResponse) error

	GetObservations(ctx context.Context,
		in *pb.GetObservationsRequest, out *pb.GetObservationsResponse) error

	GetPlacesIn(ctx context.Context,
		in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error

	GetRelatedPlaces(ctx context.Context,
		in *pb.GetRelatedPlacesRequest, out *pb.GetRelatedPlacesResponse) error

	GetInterestingPlaceAspects(ctx context.Context,
		in *pb.GetInterestingPlaceAspectsRequest, out *pb.GetInterestingPlaceAspectsResponse) error

	GetChartData(ctx context.Context,
		in *pb.GetChartDataRequest, out *pb.GetChartDataResponse) error

	GetStats(ctx context.Context,
		in *pb.GetStatsRequest, out *pb.GetStatsResponse) error
}

type store struct {
	bqDb        string
	bqClient    *bigquery.Client
	bqMapping   []*base.Mapping
	outArcInfo  map[string]map[string][]translator.OutArcInfo
	inArcInfo   map[string][]translator.InArcInfo
	subTypeMap  map[string]string
	containedIn map[util.TypePair][]string
	btTable     *bigtable.Table
	cache       *Cache
}

// randomString creates a random string with 16 runes.
func randomString() string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")
	length := 16
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

// LoadBranchCache reads the branch cache from GCS.
func (st *store) LoadBranchCache(
	ctx context.Context,
	gcsFolder string) error {
	// Cloud storage.
	log.Println("Loading cache data ...")
	newCache := map[string][]byte{}
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	it := gcsClient.Bucket(gcsBucket).Objects(ctx, &storage.Query{
		Prefix: gcsFolder + "/",
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		log.Println(attrs.Name)
		rc, err := gcsClient.Bucket(gcsBucket).Object(attrs.Name).NewReader(ctx)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		defer rc.Close()

		scanner := bufio.NewScanner(rc)
		// There is some large single cache data upto hundreds of Kb as known.
		// Set the maximum allowed buffer size to be 10Mb.
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 10*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			parts := bytes.Split(line, []byte(","))
			if len(parts) != 2 {
				log.Printf("Bad line with %d parts:\n%s", len(parts), string(line))
				continue
			}
			newCache[string(parts[0])] = parts[1]
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	st.cache.Update(newCache)
	log.Println("Branch cache load complete")
	return nil
}

// NewStore returns an implementation of Interface backed by BigQuery and BigTable.
func NewStore(
	bqDataset, btTable, btProject, btInstance, projectID, schemaPath string,
	subTypeMap map[string]string, containedIn map[util.TypePair][]string,
	branchCache bool, opts ...option.ClientOption) (Interface, error) {
	ctx := context.Background()
	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(schemaPath)
	if err != nil {
		return nil, err
	}
	mappings := []*base.Mapping{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".mcf") {
			mappingStr, err := ioutil.ReadFile(filepath.Join(schemaPath, f.Name()))
			if err != nil {
				return nil, err
			}
			mapping, err := translator.ParseMapping(string(mappingStr))
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, mapping...)
		}
	}
	outArcInfo := map[string]map[string][]translator.OutArcInfo{}
	inArcInfo := map[string][]translator.InArcInfo{}

	// Bigtable.
	btClient, err := bigtable.NewClient(ctx, btProject, btInstance, opts...)
	if err != nil {
		return nil, err
	}

	st := &store{bqDataset, bqClient, mappings, outArcInfo,
		inArcInfo, subTypeMap, containedIn, btClient.Open(btTable), NewCache()}

	if !branchCache {
		log.Println("Branch cache is not loaded.")
		return st, nil
	}

	// Cloud PubSub receiver when branch cache is updated.
	pubsubClient, err := pubsub.NewClient(ctx, pubsubProject)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	// Always create a new subscriber with default expiration date of 31 days.
	subID := subIDPrefix + randomString()
	expiration, _ := time.ParseDuration("48h")
	sub, err := pubsubClient.CreateSubscription(ctx, subID,
		pubsub.SubscriptionConfig{
			Topic:            pubsubClient.Topic(pubsubTopic),
			ExpirationPolicy: time.Duration(expiration),
		})
	if err != nil {
		log.Fatalf("pubsub CreateSubscription: %v", err)
	}
	log.Printf("Subscriber ID: %s", subID)

	// Start the receiver in a goroutine.
	go func() {
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			gcsFolder := string(msg.Data)
			log.Printf("Got message: %q\n", string(gcsFolder))
			msg.Ack()
			err := st.LoadBranchCache(ctx, gcsFolder)
			if err != nil {
				log.Printf("Load cache data got error %s", err)
			}
		})
		if err != nil {
			log.Printf("Cloud pubsub receive: %v", err)
		}
	}()

	// Initial branch cachel load.
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("%s", err)
	}
	rc, err := gcsClient.Bucket(gcsBucket).Object(versionFile).NewReader(ctx)
	if err != nil {
		log.Fatalf("%s", err)
	}
	defer rc.Close()
	gcsFolder, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("branch cache folder: %s", gcsFolder)
	err = st.LoadBranchCache(ctx, string(gcsFolder))
	if err != nil {
		log.Fatalf("Load cache data got error %s", err)
	}
	return st, nil
}

// bigTableReadRowsParallel reads BigTable rows in parallel,
// considering the size limit for RowSet is 500KB.
func bigTableReadRowsParallel(ctx context.Context, btTable *bigtable.Table,
	rowSet bigtable.RowSet, action func(row bigtable.Row) error) error {
	var rowSetSize int
	var rowList bigtable.RowList
	var rowRangeList bigtable.RowRangeList

	switch v := rowSet.(type) {
	case bigtable.RowList:
		rowList = rowSet.(bigtable.RowList)
		rowSetSize = len(rowList)
	case bigtable.RowRangeList:
		rowRangeList = rowSet.(bigtable.RowRangeList)
		rowSetSize = len(rowRangeList)
	default:
		return fmt.Errorf("unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil
	}

	errs, errCtx := errgroup.WithContext(ctx)
	rowChan := make(chan []bigtable.Row, rowSetSize)
	for i := 0; i <= rowSetSize/util.BtBatchQuerySize; i++ {
		left := i * util.BtBatchQuerySize
		right := (i + 1) * util.BtBatchQuerySize
		if right > rowSetSize {
			right = rowSetSize
		}
		var rowSetPart bigtable.RowSet
		if len(rowList) > 0 {
			rowSetPart = rowList[left:right]
		} else {
			rowSetPart = rowRangeList[left:right]
		}

		errs.Go(func() error {
			btRows := []bigtable.Row{}
			if err := btTable.ReadRows(errCtx, rowSetPart,
				func(btRow bigtable.Row) bool {
					btRows = append(btRows, btRow)
					return true
				}); err != nil {
				return err
			}
			rowChan <- btRows
			return nil
		})
	}

	err := errs.Wait()
	if err != nil {
		return err
	}
	close(rowChan)

	for rows := range rowChan {
		for _, row := range rows {
			if err := action(row); err != nil {
				return err
			}
		}
	}

	return nil
}
