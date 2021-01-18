# Mixer Load Testing

The load testing uses Locust framework. Locust master and workers are started on
staging mixer cluster and tested against staging mixer. As of 1/17/2021, staging
mixer runs on 24 pods.

## Deploy locust tasks to staging mixer

This is only needed when `docker-image/locust-tastsk/tasks.py` is changed.

Build Docker image

```bash
gcloud config set project datcom-ci
docker build -t gcr.io/datcom-ci/locust-tasks:latest ./docker-image
docker push gcr.io/datcom-ci/locust-tasks:latest
```

Deploy Locust to GKE

```bash
gcloud config set project datcom-mixer-staging
gcloud container clusters get-credentials mixer-us-central1 --region us-central1
kubectl apply -f locust-master-controller.yaml
kubectl apply -f locust-master-service.yaml
kubectl apply -f locust-worker-controller.yaml
```

Get the IP of the locust UI

```bash
echo $(kubectl get svc locust-master -o jsonpath="{.status.loadBalancer.ingress[0].ip}")
```

## Run tests and record results

Access the locust UI page at `http://[EXTERNAL_IP]:8089`

Set the `Number of total users to simulate` to 10 and `Spawn rate` to 10 then
start the test.

Let test run for about 1min, stop the test. Click on `Download Data`,
`Download Report` and save it as "{GIT_TAG}-{NUM_USERS}.html".

Repeat the test for `Number of total users to simulate` of 30 and 100.
