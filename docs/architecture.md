# System Architecture

## Overview

OrbitalSense is a telemetry processing system that simulates satellite
telemetry, transports events through Google Cloud Pub/Sub, processes and
validates the events using Apache Beam on Google Cloud Dataflow, and stores
curated telemetry in BigQuery.

The system is designed around a streaming architecture with separate
producer, transport, processing, and analytics layers.

## Architecture

```text
                         ┌─────────────────────┐
                         │  Satellite Producer  │
                         │                     │
                         │  producer/app.py    │
                         │  producer/simulator │
                         └──────────┬──────────┘
                                    │
                                    │ Telemetry events
                                    ▼
                         ┌─────────────────────┐
                         │    Google Cloud     │
                         │      Pub/Sub        │
                         │                     │
                         │  Input Topic        │
                         │  Input Subscription │
                         └──────────┬──────────┘
                                    │
                                    │ At-least-once delivery
                                    ▼
                    ┌──────────────────────────────┐
                    │ Apache Beam / Google         │
                    │ Cloud Dataflow               │
                    │                              │
                    │  1. Parse JSON               │
                    │  2. Validate schema          │
                    │  3. Validate plausibility    │
                    │  4. Add ingestion metadata   │
                    │  5. Deduplicate event IDs    │
                    │  6. Transform for BigQuery   │
                    └───────┬───────────────┬──────┘
                            │               │
                    Valid events       Invalid events
                            │               │
                            ▼               ▼
                 ┌─────────────────┐  ┌─────────────────┐
                 │    BigQuery     │  │   Dead-Letter   │
                 │                 │  │      Topic      │
                 │ Curated         │  │                 │
                 │ Telemetry       │  │ Rejected events │
                 └────────┬────────┘  └─────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │    Analytics    │
                 │                 │
                 │ SQL / Dashboards│
                 │ Monitoring      │
                 └─────────────────┘
```
