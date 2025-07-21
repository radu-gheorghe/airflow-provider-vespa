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
cp airflow-provider-vespa/example_dag.py airflow_home/dags/ 
```

Finally, run the DAG in the Airflow UI.