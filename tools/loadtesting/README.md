# Mixer load testing tool

This tool is meant for loadtesting mixer APIs in live clusters. It is based on [locust](https://docs.locust.io/en/stable/).

The specific APIs that are called are defined by a [locustfile](https://docs.locust.io/en/stable/writing-a-locustfile.html), located under `tools/loadtesting/locustfiles`. 

## Usage

If the mixer instance to be tested lives as a k8s service that is not exposed to the internet, we can run a containerized version in the same cluster.

1.  Make sure that the k8s credentials are configured. You can visit the GCP UI and click "CONNECT" on your cluster's page to get the command to configure the credentials.

    The command below should point to your cluster.
    ```sh
    kubectl config current-context
    ```

2.  Deploy the pod into the same namespace as Mixer.

    ```sh
    kubectl apply -f tools/loadtesting/pod.yaml --namespace=<mixer namespace>
    ```

3.  Make sure the pod is running with `kubectl -n <mixer namespace> get pod mixer-loadtester`. Output below means successful.

    ```sh
    NAME                                           READY   STATUS    RESTARTS      AGE
    mixer-loadtester                               1/1     Running   0             0m
    ```

4.  Forward loadtester port to localhost.

    ```sh
    kubectl -n <mixer namespace> port-forward mixer-loadtester 8089:8089
    ```

5.  Go to http://localhost:8089/ in browser. You should see the locust UI.

6.  Click "New test", set the parameters(described below) and click "Start swarming".

| Parameter | Definition |
| :---: | :---: | 
| Number of users  | Peak number of users. Note the behavior of each user is specified by a locustfile. 100 is a good default. |
| Spawn rate  | Number of users added per second. This value is recommended to be <5. |
| Host  | http:// + result of `kubectl -n <mixer namespace> get svc`. | 

## Making changes to this tool

1.  Set up the local environment.

    ```sh
    virtualenv env && source env/bin/active && pip install -r requirements.txt
    ```

2.  After desired changes (most likely edit/add of a locustfile), you may want to change the default locust file inside `Dockerfile`.

3.  Build a new image and stored it in [AR](https://cloud.google.com/artifact-registry/docs)(Below assumes that you are in the loadtesting dir.).

    ```sh
    gcloud builds submit --project=datcom-ci --tag=us-docker.pkg.dev/datcom-ci/mixer/loadtester:v2
    ```

4.  Increment both the image tag version in pod.yaml and the cloud build command above. Submit all changes into 1 PR.
