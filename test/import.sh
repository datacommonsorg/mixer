#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to import csv files into sqlite database file.

DIR=$1

if [[ $DIR == "" ]]; then
  echo "No directory specified." >&2
  echo "Usage ./import.sh test-data-dir" >&2
  exit 1
fi

set -x

sqlite3 "$DIR/datacommons.db" <<EOF
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS triples;
DROP TABLE IF EXISTS key_value_store;
.headers on
.mode csv
.import "$DIR/observations.csv" observations
.import "$DIR/triples.csv" triples
.import "$DIR/key_value_store.csv" key_value_store
EOF