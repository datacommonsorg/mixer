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
	"sync"
)

// Cache represents a Read Write locked object for in-memory key-value cache.
type Cache struct {
	sync.RWMutex
	data map[string][]byte
}

// NewCache returns a new Cache instance.
func NewCache() *Cache {
	cache := Cache{}
	cache.data = map[string][]byte{}
	return &cache
}

// Read is used for key value look up for Cache.
func (c *Cache) Read(key string) ([]byte, bool) {
	c.RLock()
	defer c.RUnlock()
	value, ok := c.data[key]
	return value, ok
}

// Update is used to update the data in Cache.
func (c *Cache) Update(data map[string][]byte) {
	c.Lock()
	defer c.Unlock()
	c.data = data
}
