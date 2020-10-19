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

package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/iterator"
)

// Memcache represents a Read Write locked object for in-memory key-value cache.
type Memcache struct {
	sync.RWMutex
	data map[string][]byte
}

// NewMemcache initialize a new Memcache instance from a key value map.
func NewMemcache(data map[string][]byte) *Memcache {
	mc := Memcache{}
	if data == nil {
		mc.data = make(map[string][]byte)
	} else {
		mc.data = data
	}
	return &mc
}

// NewMemcacheFromGCS initialize a Memcache instance from a Google Cloud Storage
// folder.
func NewMemcacheFromGCS(
	ctx context.Context, bucket, folder string) (*Memcache, error) {
	// Cloud storage.
	fmt.Printf("Enter NewMemcacheFromGCS() function")
	fmt.Printf(
		"Reading memcache data from GCS bucket: %s, folder: %s", bucket, folder)
	data := make(map[string][]byte)
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	// Iterate through the GCS folder and read the key value data.
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: folder + "/"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rc, err := client.Bucket(bucket).Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Read from file line by line and split each line by ","
		rd := bufio.NewReader(rc)
		for {
			line, err := rd.ReadBytes('\n')
			if err == io.EOF {
				break
			}
			parts := bytes.Split(line, []byte(","))
			if len(parts) != 2 {
				log.Printf("Bad line with %d parts:\n%s", len(parts), string(line))
				continue
			}
			data[string(parts[0])] = parts[1]
		}
	}
	fmt.Println("Memcache read complete")
	return NewMemcache(data), nil
}

// Read is used for key value look up for Cache.
func (m *Memcache) Read(key string) ([]byte, bool) {
	m.RLock()
	defer m.RUnlock()
	value, ok := m.data[key]
	return value, ok
}

// Update is used to update the data in Cache.
func (m *Memcache) Update(data map[string][]byte) {
	m.Lock()
	defer m.Unlock()
	m.data = data
}

// ReadParallel read multiple entries from memecache concurrently.
// This takes a function which transforms raw data into an object.
func (m *Memcache) ReadParallel(
	rowList bigtable.RowList,
	transform func(string, []byte) (interface{}, error),
	getToken func(string) (string, error),
) map[string]interface{} {
	// Channel to hold the returned object.
	elemChan := make(chan chanData, len(rowList))
	rowKeyChan := make(chan bool, maxChannelSize)

	var wg sync.WaitGroup
	for _, rowKey := range rowList {
		rowKeyChan <- true // Block if the rowKeyChan has size maxChannelSize
		wg.Add(1)
		go func(rowKey string) {
			if raw, ok := m.Read(rowKey); ok {
				if getToken == nil {
					getToken = util.KeyToDcid
				}
				token, err := getToken(rowKey)
				if err != nil {
					log.Printf("Failed to get token for rowKey %s", rowKey)
				}
				if err != nil {
					log.Printf("Invalid row key in memcache %s", rowKey)
				}
				jsonRaw, err := util.UnzipAndDecode(string(raw))
				if err != nil {
					log.Printf("Unable to unzip data for key %s", rowKey)
				}
				elem, err := transform(token, jsonRaw)
				if err != nil {
					log.Printf("Unable to process token %s, data %s", token, jsonRaw)
				}
				elemChan <- chanData{token, elem}
			}
			<-rowKeyChan
			wg.Done()
		}(rowKey)
	}
	wg.Wait()
	close(elemChan)
	result := map[string]interface{}{}
	for elem := range elemChan {
		result[elem.dcid] = elem.data
	}
	return result
}
