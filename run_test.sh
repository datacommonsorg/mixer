#!/bin/bash
# Copyright 2025 Google LLC
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


# This script is used to run all the mixer tests.

# `./run_test.sh` runs the tests with local Golang enviroment.
#   With GO_TEST_COVERAGE=1, it also writes coverage.out and coverage.xml and
#   prints overall coverage and, with COVERAGE_BASE_BRANCH set, the coverage of
#   the lines changed since that branch. The report never fails the script.
# `./run_test.sh -l` runs the linter.
# `./run_test.sh -f` runs the linter and fixes auto-fixable issues.

LINT=false
FIX=false
while getopts 'lf' c
do
  case $c in
    l) LINT=true ;;
    f) FIX=true ;;
    *) echo "Unknown option ${OPTARG}"
       exit 1
       ;;

  esac
done

set -e

REQUIRED_LINT_VERSION="2.3.0"
GOCOVER_COBERTURA="github.com/boumenot/gocover-cobertura@v1.5.0"
DIFF_COVER="diff-cover==11.0.0"
# Shared coverage scripts in datacommonsorg/.github.
SHARED_COVERAGE_SCRIPTS_URL="${SHARED_COVERAGE_SCRIPTS_URL:-https://raw.githubusercontent.com/datacommonsorg/.github/main/scripts}"

# Writes coverage.xml (Cobertura) from coverage.out, without the generated code
# in internal/proto. (-ignore-gen-files can't be used: it reads only a file's
# first 256 bytes, and the license header pushes "Code generated" past that.)
function write_cobertura_report {
  go run "$GOCOVER_COBERTURA" -ignore-dirs '(^|/)internal/proto$' \
    < coverage.out > coverage.xml
}

# Prints the overall line coverage recorded in coverage.xml.
function print_line_coverage {
  python3 - coverage.xml <<'EOF'
import sys
import xml.etree.ElementTree as ElementTree

rate = float(ElementTree.parse(sys.argv[1]).getroot().get("line-rate"))
print("#### Overall line coverage: %.1f%%" % (100 * rate))
EOF
}

# Installs diff-cover in a new venv at $1.
function install_diff_cover {
  python3 -m venv "$1" \
    && "$1/bin/pip" install -q --disable-pip-version-check "$DIFF_COVER"
}

# Prints the coverage of the lines changed since COVERAGE_BASE_BRANCH, with the
# shared report_changed_lines.sh and diff-cover in a temporary venv.
function print_changed_lines_coverage {
  if [ -z "${COVERAGE_BASE_BRANCH:-}" ]; then
    echo "Changed-line coverage not measured: not a pull request"
    return 0
  fi
  local tmp
  tmp=$(mktemp -d)
  if ! curl -fsSL --retry 3 -o "$tmp/report_changed_lines.sh" \
    "$SHARED_COVERAGE_SCRIPTS_URL/report_changed_lines.sh"; then
    echo "Changed-line coverage not measured: script download failed"
  elif ! install_diff_cover "$tmp/venv"; then
    echo "Changed-line coverage not measured: diff-cover install failed"
  else
    PATH="$tmp/venv/bin:$PATH" bash "$tmp/report_changed_lines.sh" coverage.xml
  fi
  rm -rf "$tmp"
}

# Reports the coverage that the tests wrote to coverage.out. Report-only: the
# caller ignores failures.
function report_coverage {
  write_cobertura_report || return 1
  print_line_coverage || return 1
  print_changed_lines_coverage
}

if [ "$FIX" = true ] || [ "$LINT" = true ]; then
  if ! [ -x "$(command -v golangci-lint)" ]; then
    echo 'Error: golangci-lint is not installed.' >&2
    echo "Version $REQUIRED_LINT_VERSION is required." >&2
    echo "Install it from https://golangci-lint.run/docs/welcome/install/" >&2
    exit 1
  fi

  if ! golangci-lint --version | grep -q "$REQUIRED_LINT_VERSION"; then
    echo "Error: wrong golangci-lint version. Want $REQUIRED_LINT_VERSION" >&2
    echo "Found: $(golangci-lint --version)"
    exit 1
  fi
fi

if [ "$FIX" = true ]; then
  echo "Running golangci-lint with --fix..."
  golangci-lint run --fix
elif [ "$LINT" = true ]; then
  echo "Running golangci-lint..."
  golangci-lint run
else
  echo "Running tests..."
  cover_flags=()
  if [ "${GO_TEST_COVERAGE:-}" = 1 ]; then
    cover_flags=(-covermode=set -coverpkg=./... -coverprofile=coverage.out)
  fi
  go test -v "${cover_flags[@]}" ./...
  if [ "${GO_TEST_COVERAGE:-}" = 1 ]; then
    report_coverage || echo "#### Coverage report failed (report-only)"
  fi
fi
