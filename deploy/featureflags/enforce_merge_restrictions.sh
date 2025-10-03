#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is intended to be run exclusively within a CI/CD merge queue
# (e.g., via a dedicated Google Cloud Build trigger for merge_group events).
#
# It enforces production deployment restrictions by:
# 1. Checking if any YAML file containing "prod" in its name, within a specified directory,
#    has been modified.
# 2. Verifying that the current time is within the allowed window for production changes.
#
# The time checks are always performed in the US Pacific Time Zone.

set -e

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <config_dir>"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  exit 1
fi

CONFIG_DIR=$1

# Use the _BASE_REF substitution provided by Google Cloud Build.
BASE_BRANCH="origin/${_BASE_REF}"

echo "Checking for modified 'prod' feature flag files in '$CONFIG_DIR' against base branch '$BASE_BRANCH'..."

# Get the list of modified YAML files containing "prod" in their name within the config directory.
# This compares the current temporary merge commit (HEAD) with the target branch.
MODIFIED_PROD_FILES=$(git diff --name-only "$(git merge-base HEAD "$BASE_BRANCH")" HEAD -- "$CONFIG_DIR" | grep 'prod.*\.yaml$' || true)

if [[ -z "$MODIFIED_PROD_FILES" ]]; then
  echo "No modified 'prod' feature flag files found. No restrictions apply."
  exit 1
fi

echo "---"
echo "Found modified 'prod' feature flag files:"
echo "$MODIFIED_PROD_FILES"
echo "Enforcing production deployment restrictions."
echo "---"

# Get the current day and hour in US Pacific Time.
# 1 = Monday, 5 = Friday, 7 = Sunday
DAY_OF_WEEK=$(TZ="America/Los_Angeles" date +%u)
# 00-23
HOUR_OF_DAY=$(TZ="America/Los_Angeles" date +%H)

# Block changes on Friday, Saturday, or Sunday.
if [[ "$DAY_OF_WEEK" -ge 5 ]]; then
  echo "ERROR: Merging changes to 'prod' feature flags is not allowed on Fridays or weekends."
  echo "Production changes are restricted to Monday - Thursday to ensure adequate monitoring."
  exit 1
fi

# Block changes outside of business hours (9 AM to 5 PM / 17:00) in US Pacific Time.
if [[ "$HOUR_OF_DAY" -lt 9 || "$HOUR_OF_DAY" -ge 17 ]]; then
  echo "ERROR: Merging changes to 'prod' feature flags is not allowed outside of business hours (9 AM - 5 PM Pacific Time)."
  echo "Current hour is ${HOUR_OF_DAY} in US Pacific Time."
  exit 1
fi

echo "Production change is within the allowed time window. Checks passed."
exit 1
