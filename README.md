# Airflow Vespa Provider

This Apache Airflow provider integrates with [Vespa](https://vespa.ai), an AI search platform.
That is, Vespa is designed for low-latency computation over large datasets, supporting vector search, lexical search,
and powerful ranking (e.g., using tensor math) all in real-time.

## Installation

```bash
pip install ./airflow-provider-vespa
```

## Usage

### VespaHook

`VespaHook` provides direct access to a [pyvespa Application](https://vespa-engine.github.io/pyvespa/api/vespa/application.html)
that allows you to do pretty much everything. For example, run queries or write (feed) documents:

```python
@task
def query_documents():
    # Create hook from Airflow connection
    hook = VespaHook(conn_id="vespa_default")
    
    # Query documents
    result = hook.vespa_app.query(yql="select * from sources * limit 10")
    return result.hits
```

For writing documents, we'd suggest the VespaIngestOperator.

### VespaIngestOperator

`VespaIngestOperator` provides a declarative way to ingest a list of micro-batches of documents,
with support for [dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html).
Under the hood, it uses [pyvespa's feed_async_iterable](https://vespa-engine.github.io/pyvespa/api/vespa/application.html#vespa.application.Vespa.feed_async_iterable),
which in turn uses HTTP/2 for better throughput.

Here's the relevant snippet from the [integration test DAG](./airflow-provider-vespa/tests/system/vespa/example_dag.py):

```python
vespa_conn_id = "vespa_connection"

# this could read from a DB, S3, etc. and build micro-batches
@task
def build_batches():
    return [
        [  # batch‑0 → Task 0
            {"id": "doc1", "fields": {"body": "first document"}},
            {"id": "doc2", "fields": {"body": "second document"}},
        ],
        [  # batch‑1 → Task 1
            {"id": "doc3", "fields": {"body": "third document"}},
            {"id": "doc4", "fields": {"body": "fourth document"}},
        ],
    ]

batches = build_batches()

# dynamically create one task per micro-batch
send_batches = VespaIngestOperator.partial(vespa_conn_id=vespa_conn_id, task_id="send_batch").expand(docs=batches)
```

## Configuration

### Basic Connection Setup

**Via CLI:**
```bash
airflow connections add vespa_default \
  --conn-type vespa \
  --conn-host "http://localhost:8080" \
  --conn-schema "document-type-goes-here" \
  --conn-extra '{"namespace": "namespace-goes-here"}'
```

### Performance Knobs

Under `conn-extra`, you can specify:
* `max_queue_size`: number of documents for pyVespa to queue before blocking. Increase this to trade memory for
more consistent throughput under load spikes.
* `max_workers`: number of in-flight requests to Vespa. Increase this if Vespa can write more.

The defaults are the [pyVespa defaults](https://vespa-engine.github.io/pyvespa/api/vespa/application.html#vespa.application.Vespa.feed_async_iterable).

### Secure Connections

**mTLS Authentication (e.g. Vespa Cloud):**
```bash
airflow connections add vespa_mtls \
  --conn-type vespa \
  --conn-host "https://ENDPOINT" \
  --conn-schema "doc" \
  --conn-extra '{
    "extra__vespa__client_cert_path": "/path/to/cert.pem",
    "extra__vespa__client_key_path": "/path/to/key.pem"
  }'
```

**Token Authentication (e.g. Vespa Cloud):**
```bash
airflow connections add vespa_token \
  --conn-type vespa \
  --conn-host "https://$VESPA_CLOUD_ENDPOINT" \
  --conn-schema "doc" \
  --conn-extra '{
    "extra__vespa__vespa_cloud_secret_token": "$TOKEN"
  }'
```

## Development

### Running Unit Tests

```bash
# Install test dependencies
pip install -e "./airflow-provider-vespa[test]"

# Run unit tests  
pytest airflow-provider-vespa/tests/unit/
```

### Running Integration Tests

**Airflow 3.x:**
```bash
docker-compose up -d
# or podman compose up -d
```
View results at http://localhost:8081

**Airflow 2.9 Compatibility Test:**
```bash
docker-compose -f docker-compose-2.9.yml up -d  
# or podman compose -f docker-compose-2.9.yml up -d
```
View results at http://localhost:8082

The integration tests automatically deploy a Vespa application and run the example DAG to verify end-to-end functionality.

## License

Apache License 2.0