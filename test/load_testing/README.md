# Mixer Load Testing

The load testing uses [Locust framework](https://docs.locust.io/en/stable/quickstart.html).
Locust master and workers are started on staging mixer cluster and tested against staging mixer.
As of 1/17/2021, staging mixer runs on 24 pods.

## Deploy locust tasks to staging mixer

This is only needed when `docker-image/locust-tasks/tasks.py` is changed.

### Build Docker image

```bash
gcloud config set project datcom-ci
docker build -t gcr.io/datcom-ci/locust-tasks:latest ./docker-image
docker push gcr.io/datcom-ci/locust-tasks:latest
```

### Deploy Locust to GKE

```bash
gcloud config set project datcom-mixer-staging
gcloud container clusters get-credentials mixer-us-central1 --region us-central1
kubectl apply -f locust-master-controller.yaml
kubectl apply -f locust-master-service.yaml
kubectl apply -f locust-worker-controller.yaml
```

### Get the IP of the locust UI

```bash
echo $(kubectl get svc locust-master -o jsonpath="{.status.loadBalancer.ingress[0].ip}")
```

## Run tests and record results

Access the locust UI page at `http://[EXTERNAL_IP]:8089`

Set the `Number of total users to simulate` to 10 and `Spawn rate` to 10 then
start the test.

Let test run for about 1min, stop the test. Click on `Download Data`,
`Download Report`, save it as "{GIT_TAG}-{NUM_USERS}.html" and check in the
html page in a PR.

In the report, you should expect to see "0 Fails" for all the requests and about 20
"Aggreagated RPS".

Repeat the test for `Number of total users to simulate` of 30 and 100.

## View past reports

Use [htmlpreview](https://htmlpreview.github.io/) to view the past report.

An example [report](http://htmlpreview.github.io/?https://github.com/datacommonsorg/mixer/blob/master/test/load_testing/reports/bbc418e-10.html).
