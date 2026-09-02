# OrbitalSense Telemetry Producer

A Python-based telemetry simulator for OrbitalSense that simulates 12 satellites communicating through 4 ground stations.

The producer generates realistic satellite subsystem telemetry and publishes events to Google Cloud Pub/Sub.

## Architecture

```text
12 Satellites
      |
      | telemetry
      v
Telemetry Producer
      |
      | Google Cloud Pub/Sub
      v
events-dev topic
      |
      v
events-dev-subscription
      |
      v
Telemetry Consumer
      |
      +--> Raw events
      |       |
      |       v
      |   BigQuery
      |   analytics_dev.telemetry_raw
      |
      +--> Valid events
      |       |
      |       v
      |   Deduplication
      |       |
      |       v
      |   BigQuery
      |   analytics_dev.events
      |
      +--> Invalid events
              |
              v
          Dead-letter topic
          events-dev-dlq
```

## Project Structure

```text
app/
├── producer/
│   └── ...
├── consumer/
│   ├── pipeline.py
│   └── validation.py
├── README.md
├── setup.py
└── ...
```

## Environment Variables

The producer and consumer use environment variables for Google Cloud and Pub/Sub configuration.

### Required variables

| Variable             | Description                                                   | Example                                                            |
| -------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| `PROJECT_ID`         | Google Cloud project ID                                       | `orbitalsense-2026`                                                |
| `PUBSUB_TOPIC`       | Pub/Sub topic used by the producer                            | `events-dev`                                                       |
| `INPUT_SUBSCRIPTION` | Fully-qualified Pub/Sub subscription consumed by the pipeline | `projects/orbitalsense-2026/subscriptions/events-dev-subscription` |
| `DLQ_TOPIC`          | Fully-qualified Pub/Sub dead-letter topic                     | `projects/orbitalsense-2026/topics/events-dev-dlq`                 |
| `BIGQUERY_TABLE`     | Destination table for validated/curated events                | `orbitalsense-2026:analytics_dev.events`                           |
| `RAW_BIGQUERY_TABLE` | Destination table for raw Pub/Sub messages                    | `orbitalsense-2026:analytics_dev.telemetry_raw`                    |

### Optional variables

| Variable           | Description                                             | Default |
| ------------------ | ------------------------------------------------------- | ------- |
| `PIPELINE_VERSION` | Version recorded on curated events                      | `1.0.0` |
| `PYTHONUNBUFFERED` | Disables Python stdout buffering when running in Docker | `1`     |

### Local environment

For local development, export the variables before running the consumer:

```bash
export PROJECT_ID=orbitalsense-2026

export INPUT_SUBSCRIPTION=projects/orbitalsense-2026/subscriptions/events-dev-subscription

export DLQ_TOPIC=projects/orbitalsense-2026/topics/events-dev-dlq

export BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.events

export RAW_BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.telemetry_raw

export PIPELINE_VERSION=1.0.0
```

Verify the variables:

```bash
echo "$PROJECT_ID"
echo "$INPUT_SUBSCRIPTION"
echo "$DLQ_TOPIC"
echo "$BIGQUERY_TABLE"
echo "$RAW_BIGQUERY_TABLE"
echo "$PIPELINE_VERSION"
```

## Google Cloud Authentication

The consumer requires Google Cloud Application Default Credentials (ADC).

Authenticate locally with:

```bash
gcloud auth application-default login
```

Verify ADC:

```bash
gcloud auth application-default print-access-token >/dev/null && echo "ADC OK"
```

The expected output is:

```text
ADC OK
```

Make sure the authenticated identity has permission to:

- Consume messages from the Pub/Sub subscription
- Publish messages to the DLQ topic
- Write to the BigQuery datasets/tables
- Run Dataflow jobs when using `DataflowRunner`

## Pub/Sub Configuration

The producer publishes to:

```text
projects/orbitalsense-2026/topics/events-dev
```

The consumer reads from:

```text
projects/orbitalsense-2026/subscriptions/events-dev-subscription
```

The dead-letter topic is:

```text
projects/orbitalsense-2026/topics/events-dev-dlq
```

Verify the subscription:

```bash
gcloud pubsub subscriptions describe events-dev-subscription \
  --project="$PROJECT_ID"
```

List subscriptions:

```bash
gcloud pubsub subscriptions list \
  --project="$PROJECT_ID" \
  --format="table(name,topic)"
```

Verify the topic:

```bash
gcloud pubsub topics describe events-dev \
  --project="$PROJECT_ID"
```

## BigQuery Configuration

The consumer writes raw Pub/Sub messages to:

```text
orbitalsense-2026.analytics_dev.telemetry_raw
```

Validated and deduplicated events are written to:

```text
orbitalsense-2026.analytics_dev.events
```

The BigQuery dataset can be inspected with:

```bash
bq ls --project_id="$PROJECT_ID"
```

List the tables:

```bash
bq ls --project_id="$PROJECT_ID" analytics_dev
```

Expected tables include:

```text
events
telemetry
telemetry_raw
telemetry_staging
```

Check the raw event count:

```bash
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  '
  SELECT COUNT(*) AS row_count
  FROM `orbitalsense-2026.analytics_dev.telemetry_raw`
  '
```

Check the curated event count:

```bash
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  '
  SELECT COUNT(*) AS row_count
  FROM `orbitalsense-2026.analytics_dev.events`
  '
```

## Running the Consumer Locally

The consumer is an Apache Beam streaming pipeline.

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Set the required environment variables:

```bash
export PROJECT_ID=orbitalsense-2026

export INPUT_SUBSCRIPTION=projects/orbitalsense-2026/subscriptions/events-dev-subscription

export DLQ_TOPIC=projects/orbitalsense-2026/topics/events-dev-dlq

export BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.events

export RAW_BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.telemetry_raw
```

Then run the consumer with the local DirectRunner:

```bash
python -m consumer.pipeline \
  --runner=DirectRunner \
  --streaming
```

### Important: DirectRunner and streaming

The streaming pipeline uses cross-language Beam transforms, including the BigQuery Storage Write API.

The Python DirectRunner does not support this streaming cross-language pipeline configuration.

If you see:

```text
RuntimeError: Streaming Python direct runner does not support
cross-language pipelines.
```

run the pipeline with `DataflowRunner` instead.

## Running the Consumer on Dataflow

Submit the streaming pipeline to Dataflow:

```bash
python -m consumer.pipeline \
  --runner=DataflowRunner \
  --streaming \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --setup_file=./setup.py
```

The pipeline will:

1. Read telemetry from Pub/Sub.
2. Write the original Pub/Sub messages to `telemetry_raw`.
3. Parse and validate the telemetry.
4. Send invalid events to the DLQ topic.
5. Deduplicate valid events by `event_id`.
6. Write curated events to `analytics_dev.events`.

### Dataflow job status

List Dataflow jobs:

```bash
gcloud dataflow jobs list \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --limit=10 \
  --format="table(id,name,state,creationTime)"
```

List only currently running jobs:

```bash
gcloud dataflow jobs list \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --filter="state=RUNNING" \
  --format="table(id,name,state,creationTime)"
```

Describe a job:

```bash
gcloud dataflow jobs describe JOB_ID \
  --project="$PROJECT_ID" \
  --region=europe-west1
```

Check Dataflow errors:

```bash
gcloud logging read \
  'resource.type="dataflow_step"
   resource.labels.job_id="JOB_ID"
   severity>=ERROR' \
  --project="$PROJECT_ID" \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"
```

Replace `JOB_ID` with the actual Dataflow job ID.

## Dataflow Networking

The Dataflow worker subnet must have Private Google Access enabled when required by the worker networking configuration.

For the default `europe-west1` subnet:

```bash
gcloud compute networks subnets update default \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --enable-private-ip-google-access
```

Verify:

```bash
gcloud compute networks subnets describe default \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --format="yaml(name,region,privateIpGoogleAccess)"
```

## Dataflow Worker Capacity

If Dataflow reports:

```text
QUOTA_EXCEEDED: Quota 'IN_USE_ADDRESSES' exceeded
```

or:

```text
ZONE_RESOURCE_POOL_EXHAUSTED
```

the pipeline itself may be valid, but Dataflow cannot provision workers.

For example:

```text
The zone 'europe-west1-b' does not have enough resources available
to fulfill the request.
```

or:

```text
The zone 'europe-west1-c' does not have enough resources available
to fulfill the request.
```

This is a Dataflow/Compute Engine capacity or quota problem rather than a Pub/Sub or Beam pipeline configuration error.

Check the current Dataflow jobs:

```bash
gcloud dataflow jobs list \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --limit=10 \
  --format="table(id,name,state,creationTime)"
```

Avoid leaving multiple obsolete streaming jobs running. Drain older jobs when they are no longer required.

## Running the Producer with Docker

The telemetry producer is packaged as a Docker image.

The image is:

```text
europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-producer:latest
```

The producer requires a Google Cloud service-account key when running outside the Google Cloud environment.

The credentials file is mounted into the container as:

```text
/tmp/producer-key.json
```

Run the producer:

```bash
docker run --rm \
  --env-file producer/.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/producer-key.json \
  -v "$(pwd)/producer/producer-key.json:/credentials/producer-key.json:ro" \
  europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-producer:latest
```

Replace:

```text
/path/to/producer-key.json
```

with the actual location of the credentials file.

### Verify the credentials file before running Docker

Make sure the path exists and is a file:

```bash
ls -l /path/to/producer-key.json
```

It should look similar to:

```text
-rw-------  1 user  staff  ... producer-key.json
```

If the path is a directory, Docker will mount the directory instead of the JSON file. The producer will then fail with:

```text
IsADirectoryError: [Errno 21] Is a directory:
'/tmp/producer-key.json'
```

## End-to-End Flow

Start the consumer first:

```bash
docker run --rm \
  --env-file consumer/.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/application_default_credentials.json \
  -v "$HOME/.config/gcloud/application_default_credentials.json:/tmp/application_default_credentials.json:ro" \
  europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-consumer:latest \
  python -m consumer.pipeline
```

Confirm the Dataflow job is running:

```bash
gcloud dataflow jobs list \
  --project="$PROJECT_ID" \
  --region=europe-west1 \
  --limit=5 \
  --format="table(id,name,state,creationTime)"
```

Then start the producer:

```bash
docker run --rm \
  -e PYTHONUNBUFFERED=1 \
  -e PROJECT_ID=orbitalsense-2026 \
  -e PUBSUB_TOPIC=events-dev \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/producer-key.json \
  -v /path/to/producer-key.json:/tmp/producer-key.json:ro \
  europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-producer:latest
```

The expected data flow is:

```text
Docker Producer
      |
      v
Pub/Sub: events-dev
      |
      v
events-dev-subscription
      |
      v
Dataflow Consumer
      |
      +--------------------+
      |                    |
      v                    v
telemetry_raw          validation
                           |
                    +------+------+
                    |             |
                  valid         invalid
                    |             |
                    v             v
                dedupe          DLQ
                    |
                    v
             analytics_dev.events
```

## Troubleshooting

### Subscription not found

If Dataflow reports:

```text
NOT_FOUND: Unable to find subscription
projects/orbitalsense-2026/subscriptions/events-dev-sub
```

check the actual subscription name:

```bash
gcloud pubsub subscriptions list \
  --project="$PROJECT_ID" \
  --format="table(name,topic)"
```

The configured subscription must exactly match the fully-qualified resource:

```text
projects/orbitalsense-2026/subscriptions/events-dev-subscription
```

### BigQuery table reference error

Beam expects BigQuery table references in one of these forms:

```text
PROJECT:DATASET.TABLE
```

or:

```text
DATASET.TABLE
```

Correct:

```bash
export BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.events

export RAW_BIGQUERY_TABLE=orbitalsense-2026:analytics_dev.telemetry_raw
```

Do not use a filesystem path such as:

```text
/Users/.../orbitalsense-2026:analytics_dev.events
```

### Raw table receives data but curated table remains empty

Check the two tables separately:

```bash
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  '
  SELECT COUNT(*) AS row_count
  FROM `orbitalsense-2026.analytics_dev.telemetry_raw`
  '
```

and:

```bash
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  '
  SELECT COUNT(*) AS row_count
  FROM `orbitalsense-2026.analytics_dev.events`
  '
```

If `telemetry_raw` increases while `events` remains at zero, inspect the Dataflow worker logs for validation, serialization, deduplication, or BigQuery write errors.

### Dataflow worker startup failures

If the job remains `RUNNING` but logs repeatedly contain:

```text
ZONE_RESOURCE_POOL_EXHAUSTED
```

Dataflow is having difficulty obtaining worker capacity in the selected zone.

If the logs contain:

```text
IN_USE_ADDRESSES exceeded
```

the project has reached its regional external-IP address quota.

Check active Compute Engine instances and regional quota before starting additional streaming jobs.

## Current BigQuery Tables

The development dataset is:

```text
orbitalsense-2026.analytics_dev
```

Relevant tables:

```text
analytics_dev.events
analytics_dev.telemetry_raw
analytics_dev.telemetry
analytics_dev.telemetry_staging
```

The primary streaming pipeline destinations are:

```text
analytics_dev.telemetry_raw
analytics_dev.events
```

## Pipeline Processing

The consumer performs the following processing stages:

```text
Pub/Sub Message
      |
      v
Raw BigQuery Write
      |
      v
JSON Parsing
      |
      v
Event Validation
      |
      +------ invalid ------> DLQ
      |
      v
Key by event_id
      |
      v
24-hour Deduplication
      |
      v
BigQuery Row Conversion
      |
      v
Curated BigQuery Table
```

Invalid events contain diagnostic information such as:

```text
reason_code
reason_detail
pipeline_version
ingestion_timestamp
original_event
```

Valid events contain pipeline metadata including:

```text
ingestion_timestamp
pipeline_version
```

## Development Notes

The consumer uses Apache Beam with Python 3.13.

Current Beam version:

```text
2.75.0
```

The production streaming runner is:

```text
DataflowRunner
```

The local runner is useful for development, but streaming pipelines that depend on cross-language transforms should be executed with Dataflow or another supported runner.
