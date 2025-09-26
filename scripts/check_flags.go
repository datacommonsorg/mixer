// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ensures that a file will not throw errors when parsing.
// Note that the set of keys in a valid yaml is not guaranteed to match the set
// of keys in the live feature flag struct.

package main

import (
	"fmt"
	"os"

	"github.com/datacommonsorg/mixer/internal/featureflags"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: go run scripts/check_flags.go <file-path>")
		os.Exit(1)
	}
	_, err := featureflags.NewFlags(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing feature flags file %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	fmt.Printf("Successfully validated %s\n", os.Args[1])
}
