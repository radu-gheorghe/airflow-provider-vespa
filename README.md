# This is a work in progress

# Testing locally

Requirements: Docker/Podman

## Start Vespa

Normally, Vespa's query/docs port is 8080, but since Airflow uses it by default, we'll go with 8082.

```bash
podman run --name vespa-airflow --hostname vespa-airflow \                                             
  --publish 8082:8080 --publish 19071:19071 \
  vespaengine/vespa
```

## Deploy the application package

This contains all the config and get Vespa ready. For now, we only have one field, `body`.

Ideally, you'd do this with the [Vespa CLI](https://docs.vespa.ai/en/vespa-cli.html). On a Mac, you'd do:

```bash
brew install vespa-cli
```

Otherwise, you should find the binary in the [Releases page](https://github.com/vespa-engine/vespa/releases).

Now you can simply run:

```bash
cd vespa_app
vespa deploy
```

Once container logging settles, it's ready.

## Basic checks

Have a look at the [queries.http](./queries.http) file. It assumes you're seeing it in VSCode/friends with the [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension.

## Deploying the sample DAG

What I normally do, at least :)

First, install the provider:

```bash
pip install -e ./airflow-provider-vespa
```

Then copy the DAG to the Airflow DAGs folder:

```bash
cp airflow-provider-vespa/example_dag.py $AIRFLOW_HOME/dags/ 
```

Finally, run the DAG in the Airflow UI. Note that forks might be...forked... on OSX, so you'll have to do something like this in order for deferred tasks to work:
```bash
export no_proxy="*"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export MULTIPROCESSING_START_METHOD=spawn
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor

airflow standalone
```

## Securing the connection (e.g. for Vespa Cloud)

Example mTLS connection (replace the variables with your own):

```bash
airflow connections add vespa_mtls --conn-type vespa --conn-host "https://$VESPA_CLOUD_ENDPOINT" --conn-schema "doc" --conn-extra '{"extra__vespa__client_cert_path": "/Users/radu/.vespa/'$VESPA_CLOUD_APP_NAME'/data-plane-public-cert.pem", "extra__vespa__client_key_path": "/Users/radu/.vespa/'$VESPA_CLOUD_APP_NAME'/data-plane-private-key.pem"}'
```

Example token connection:

```bash
airflow connections add vespa_token --conn-type vespa --conn-host "https://$VESPA_CLOUD_ENDPOINT" --conn-schema "doc" --conn-extra '{"extra__vespa__vespa_cloud_secret_token": "'$VESPA_CLOUD_SECRET_TOKEN'"}'
```

## Running Unit Tests

To run the unit tests for the Airflow provider:

1. Install test dependencies:
   ```bash
   pip install -e "./airflow-provider-vespa[test]"
   ```

2. Run unit tests:
   ```bash
   pytest airflow-provider-vespa/tests/unit/
   ```
   
## Running Integration Tests

### Airflow 3.x (Current)

   ```bash
   docker-compose up -d
   # or podman compose up -d
   ```

   Then check that the example DAG ran successfully at http://localhost:8081

### Airflow 2.9 Integration Test

   To test compatibility with Airflow 2.9:

   ```bash
   docker-compose -f docker-compose-2.9.yml up -d
   # or podman compose -f docker-compose-2.9.yml up -d
   ```

   Then check that the example DAG ran successfully at http://localhost:8082

   **Key differences in Airflow 2.9 setup:**
   - Uses `webserver` instead of `api-server`
   - Different authentication configuration
   - Runs on port 8082 to avoid conflicts
   - Uses separate PostgreSQL volume (`postgres-db-volume-2-9`)