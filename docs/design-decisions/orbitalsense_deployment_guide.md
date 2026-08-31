# OrbitalSense Telemetry Pipeline — Deployment Guide

## 1. Overview

This guide covers prerequisites, deployment, configuration, execution, verification, rollback, and teardown for the OrbitalSense telemetry pipeline.

The current deployment consists of:

- Google Cloud Pub/Sub for telemetry ingestion
- Apache Beam running on Google Cloud Dataflow for streaming processing
- BigQuery for raw and processed telemetry
- Artifact Registry for the consumer container
- Dataflow region: `europe-west2`

## 2. Prerequisites

Install and authenticate:

- Docker
- Google Cloud CLI (`gcloud`)
- BigQuery CLI (`bq`)
- Python 3.13/project virtual environment if running outside the container

Authenticate and select the project:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project orbitalsense-2026
```

Verify:

```bash
gcloud config get-value project
gcloud auth list
gcloud auth application-default print-access-token >/dev/null && echo "ADC OK"
```

Required Google Cloud APIs include Dataflow, Compute Engine, Pub/Sub, BigQuery, and Artifact Registry. The deploying identity must have permissions to submit/manage Dataflow jobs and access the relevant resources.

## 3. Configuration

Environment-specific settings are supplied through:

```text
consumer/.env
```

Keep credentials and secrets out of source control.

For local Dataflow submission, the ADC file is mounted into Docker:

```bash
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/application_default_credentials.json \\
-v "$HOME/.config/gcloud/application_default_credentials.json:/tmp/application_default_credentials.json:ro"
```

For production, prefer a dedicated service identity/workload identity instead of mounting a developer's ADC file.

## 4. Build and publish the consumer image

Authenticate Docker:

```bash
gcloud auth configure-docker europe-west1-docker.pkg.dev
```

Build:

```bash
docker build -t europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-consumer:latest .
```

Push:

```bash
docker push europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-consumer:latest
```

For production/reproducible deployments, prefer immutable version tags or image digests instead of `latest`.

## 5. Deploy and execute

Before starting a new streaming job, check existing active jobs:

```bash
gcloud dataflow jobs list \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --status=active \\
  --sort-by=~createTime
```

Run the consumer:

```bash
docker run --rm \\
  --env-file consumer/.env \\
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/application_default_credentials.json \\
  -v "$HOME/.config/gcloud/application_default_credentials.json:/tmp/application_default_credentials.json:ro" \\
  europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-consumer:latest \\
  python -m consumer.pipeline
```

The Apache Beam warning about downloading the Java expansion-service JAR at runtime is a security/reproducibility warning, not by itself a Dataflow failure. The missing README warning from `sdist` is likewise a packaging warning.

## 6. Verify deployment

### Dataflow

```bash
gcloud dataflow jobs list \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --status=active \\
  --sort-by=~createTime
```

Describe the intended job:

```bash
gcloud dataflow jobs describe \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(id,name,currentState,currentStateTime)"
```

Normally there should be one authoritative streaming job. Multiple copies can cause duplicate processing and unnecessary resource consumption.

### Raw BigQuery ingestion

```bash
bq query --use_legacy_sql=false '
SELECT
  COUNT(*) AS raw_rows_last_10_minutes,
  MAX(ingestion_timestamp) AS latest_raw_ingestion
FROM `orbitalsense-2026.analytics_dev.telemetry_raw`
WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
'
```

### Processed BigQuery ingestion

```bash
bq query --use_legacy_sql=false '
SELECT
  COUNT(*) AS telemetry_rows_last_10_minutes,
  MAX(ingestion_timestamp) AS latest_telemetry_ingestion,
  MAX(pipeline_version) AS pipeline_version
FROM `orbitalsense-2026.analytics_dev.telemetry`
WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
'
```

### End-to-end test message

For a known Pub/Sub message ID:

```bash
bq query --use_legacy_sql=false '
SELECT
  message_id,
  publish_timestamp,
  ingestion_timestamp,
  payload
FROM `orbitalsense-2026.analytics_dev.telemetry_raw`
WHERE message_id = "MESSAGE_ID"
'
```

Then verify the corresponding processed record using the event ID/fields produced by the pipeline.

## 7. Resource/quota troubleshooting

A previous submission failed with:

```text
Project orbitalsense-2026 has insufficient resource(s) to execute this workflow
with 1 instances in region europe-west2.
```

Check regional Compute Engine quota and current usage:

```bash
gcloud compute regions describe europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(quotas)"
```

Do not assume that deleting BigQuery data will fix this error. The relevant resources are Compute Engine/Dataflow worker resources, addresses, disks, instances, and related regional capacity.

Before redeploying, make sure obsolete Dataflow streaming jobs have stopped. A `Draining` or `Cancelling` job can temporarily continue consuming resources until it fully terminates.

## 8. Rollback

### Step 1 — Identify the job

```bash
gcloud dataflow jobs list \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --status=active \\
  --sort-by=~createTime
```

Use the exact job ID returned by this command.

### Step 2 — Drain the faulty streaming job

```bash
gcloud dataflow jobs drain \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026
```

Monitor it:

```bash
gcloud dataflow jobs describe \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(currentState,currentStateTime)"
```

Wait until the job is no longer active before starting another copy when single-job operation is intended.

If a drain/cancel command reports that the job does not exist or permissions are insufficient, verify the active account, project, region, and exact job ID:

```bash
gcloud auth list
gcloud config get-value account
gcloud config get-value project
```

### Step 3 — Deploy the previous known-good image

Use an immutable previous version, for example:

```text
telemetry-consumer:1.0.0
```

Then:

```bash
docker run --rm \\
  --env-file consumer/.env \\
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/application_default_credentials.json \\
  -v "$HOME/.config/gcloud/application_default_credentials.json:/tmp/application_default_credentials.json:ro" \\
  europe-west1-docker.pkg.dev/orbitalsense-2026/containers-dev/telemetry-consumer:PREVIOUS_VERSION \\
  python -m consumer.pipeline
```

Verify Dataflow and BigQuery again.

## 9. Teardown

Use teardown only when the streaming pipeline is intentionally being decommissioned.

### Stop Dataflow

```bash
gcloud dataflow jobs list \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --status=active
```

Drain the intended job:

```bash
gcloud dataflow jobs drain \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026
```

Wait until it leaves the active list.

### Confirm resource release

```bash
gcloud compute regions describe europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(quotas)"
```

Check `CPUS`, `INSTANCES`, `IN_USE_ADDRESSES`, and `DISKS_TOTAL_GB`.

### Clean up artifacts carefully

Delete obsolete container versions only when they are no longer needed for rollback. Do not delete the active or known-good rollback image.

Do not delete BigQuery tables merely to resolve Dataflow/Compute Engine quota pressure. If historical telemetry must be removed, use the project's approved BigQuery retention/lifecycle policy.

Do not delete Pub/Sub topics/subscriptions unless the pipeline is permanently decommissioned and they are no longer required.

## 10. Operational checklist

### Before deployment

- [ ] Correct project selected: `orbitalsense-2026`
- [ ] ADC/service identity authenticated
- [ ] Required APIs enabled
- [ ] Pub/Sub resources and permissions verified
- [ ] BigQuery dataset/tables verified
- [ ] Container image exists in Artifact Registry
- [ ] `consumer/.env` contains intended configuration
- [ ] Existing Dataflow jobs reviewed
- [ ] Regional resource/quota state checked

### After deployment

- [ ] Intended Dataflow job is `RUNNING`
- [ ] No unintended duplicate streaming job is active
- [ ] `telemetry_raw` receives recent records
- [ ] `telemetry` receives recent records
- [ ] `pipeline_version` is correct
- [ ] Known test message can be traced end-to-end
- [ ] Duplicate/malformed-record behavior is verified according to the architecture

### Rollback

- [ ] Exact faulty job ID identified
- [ ] Faulty job drained/stopped
- [ ] Job removed from active list
- [ ] Previous known-good image deployed
- [ ] Raw and processed ingestion verified
- [ ] Faulty image/version retained until incident closure

## 11. Quick reference

### Active jobs

```bash
gcloud dataflow jobs list \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --status=active \\
  --sort-by=~createTime
```

### Job status

```bash
gcloud dataflow jobs describe \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(id,name,currentState,currentStateTime)"
```

### Regional quotas

```bash
gcloud compute regions describe europe-west2 \\
  --project=orbitalsense-2026 \\
  --format="yaml(quotas)"
```

### Drain

```bash
gcloud dataflow jobs drain \\
  JOB_ID \\
  --region=europe-west2 \\
  --project=orbitalsense-2026
```
