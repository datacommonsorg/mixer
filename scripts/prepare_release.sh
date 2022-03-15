#!/bin/bash
#
# A helper script to update the storage versions and prepare a commit for
# release.
#

DIR=$(dirname "$0")

cd "$DIR"/..
VERSION=

function update_version() {
  echo ""
  echo "==== Updating BT and BQ versions ===="

  for src in $(gsutil ls gs://datcom-control/autopush/*_latest_base_cache_version.txt); do
    echo "Copying $src"
    echo "$(gsutil cat $src)" >> deploy/storage/bigtable_import_groups.version
  done

  BQ=$(curl https://autopush.datacommons.org/version 2>/dev/null | awk '{ if ($1~/^datcom-store/) print $1; }')
  printf "$BQ" > deploy/storage/bigquery.version

  VERSION="${BT//borgcron_/}"
}

function update_proto() {
  echo ""
  echo "==== Updating go proto files ===="
  protoc --proto_path=proto --go_out=internal --go-grpc_out=internal \
         --go-grpc_opt=requireUnimplementedServers=false proto/*.proto
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to update proto"
    exit 1
  fi
}

function update_golden() {
  echo ""
  echo "==== Updating staging golden files ===="
  ./scripts/update_golden.sh
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to update proto"
    exit 1
  fi
}

function commit() {
  echo ""
  echo "==== Committing the change ===="
  git commit -a -m "Data Release: $VERSION"
}

# update_version
update_proto
update_golden
commit

echo ""
echo "NOTE: Please review the commit, push to your remote repo and create a PR."
echo ""
