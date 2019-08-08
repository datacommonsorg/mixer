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

package sparql

import (
	"strings"
	"testing"

	"github.com/go-test/deep"
)

func TestReader(t *testing.T) {
	s := "str"
	want := []rune{'s', 't', 't'}
	result := []rune{}
	testReader := reader{r: strings.NewReader(s)}

	ch, _, err := testReader.ReadRune()
	if err != nil {
		t.Fatalf("ReadRune error: %s", err)
	}
	result = append(result, ch)

	ch, _, err = testReader.ReadRune()
	if err != nil {
		t.Fatalf("ReadRune error: %s", err)
	}
	result = append(result, ch)

	err = testReader.UnreadRune()
	if err != nil {
		t.Fatalf("UnreadRune error: %s", err)
	}

	ch, _, err = testReader.ReadRune()
	if err != nil {
		t.Fatalf("ReadRune error: %s", err)
	}
	result = append(result, ch)

	if diff := deep.Equal(want, result); diff != nil {
		t.Errorf("Unexpected diff %v", diff)
	}
}
