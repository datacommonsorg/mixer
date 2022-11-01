#!/bin/bash
#
# Copyright 2020 Google LLC
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


# Send golden diff email for a new Bigtable cache.

set -e

BASE_BIGTABLE_INFO=$1

apt-get update -y
apt-get install -y gawk

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

echo "$BASE_BIGTABLE_INFO" | tee "$ROOT/deploy/storage/base_bigtable_info.yaml"

# Script to convert terminal colors and attributes to HTML
# https://github.com/pixelb/scripts/blob/master/scripts/ansi2html.sh
wget "http://www.pixelbeat.org/scripts/ansi2html.sh" -O /tmp/ansi2html.sh
chmod +x /tmp/ansi2html.sh


"$ROOT/scripts/update_golden.sh" | /tmp/ansi2html.sh > /tmp/golden-diff.html

if ! grep -Fxq "FAIL" /tmp/golden-diff.html; then
  git diff | /tmp/ansi2html.sh > /tmp/golden-diff.html
fi
