# Go-protoc Docker Image

## Description

This is a Docker image with the base dependencies for Mixer server at the
expected versions:
- golang
- protoc
- protoc-gen-go
- protoc-gen-go-grpc

## How to update the Docker image

To generate the Docker image and push it to GCS:

1. Change the version string in cloudbuild.yaml
2. Search for other occurrences of the version string and update those too.
   Currently this includes:
   - build/ci/cloudbuild.push.yaml
   - build/ci/cloudbuild.test.yaml
3. Run (from this directory):

```bash
gcloud config set project datcom-ci
gcloud builds submit . --config=cloudbuild.yaml
```
