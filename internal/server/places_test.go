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
	"reflect"
	"testing"
)

func TestKeysToSlice(t *testing.T) {
	m := map[string]bool{
		"1": true,
		"2": true,
		"3": true,
	}
	expected := []string{"1", "2", "3"}
	result := keysToSlice(m)
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("places.keysToSlice(%v) = %v; expected %v", m, result, expected)
	}
}
