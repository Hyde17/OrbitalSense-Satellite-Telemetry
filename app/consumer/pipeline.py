import json
import os
from datetime import datetime, date, timezone
from typing import Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import Timestamp

from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from consumer.helper import to_beam_timestamp
from consumer.validation import validate_event


PROJECT_ID = os.environ["PROJECT_ID"]
INPUT_SUBSCRIPTION = os.environ["INPUT_SUBSCRIPTION"]
DLQ_TOPIC = os.environ["DLQ_TOPIC"]
BIGQUERY_TABLE = os.environ["BIGQUERY_TABLE"]
RAW_BIGQUERY_TABLE = os.environ["RAW_BIGQUERY_TABLE"]

RAW_BIGQUERY_SCHEMA = {
    "fields": [
        {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publish_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "payload", "type": "STRING", "mode": "REQUIRED"},
    ]
}

BIGQUERY_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "satellite_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ground_station_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sequence_number", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "subsystem", "type": "STRING", "mode": "NULLABLE"},
        {"name": "telemetry", "type": "STRING", "mode": "NULLABLE"},
        {"name": "position", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "pipeline_version", "type": "STRING", "mode": "REQUIRED"},
        {"name": "source_ground_station", "type": "STRING", "mode": "NULLABLE"},
    ]
}

PIPELINE_VERSION = os.getenv(
    "PIPELINE_VERSION",
    "1.0.0",
)

BQ_WRITE_METHOD = os.getenv(
    "BQ_WRITE_METHOD",
    "STORAGE_WRITE_API",
)


def get_bq_write_method():
    if BQ_WRITE_METHOD == "FILE_LOADS":
        return beam.io.WriteToBigQuery.Method.FILE_LOADS

    if BQ_WRITE_METHOD == "STORAGE_WRITE_API":
        return beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API

    raise ValueError("BQ_WRITE_METHOD must be FILE_LOADS or STORAGE_WRITE_API")


class ParseAndValidate(beam.DoFn):
    VALID = "valid"
    INVALID = "invalid"

    def process(self, message):
        ingestion_timestamp = datetime.now(timezone.utc)

        try:
            event = json.loads(message.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                {
                    "reason_code": "INVALID_JSON",
                    "reason_detail": "Unable to decode JSON",
                    "pipeline_version": PIPELINE_VERSION,
                    "ingestion_timestamp": ingestion_timestamp,
                    "original_event": message.data.decode(
                        "utf-8",
                        errors="replace",
                    ),
                },
            )
            return

        valid, reason = validate_event(event)

        if not valid:
            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                {
                    "reason_code": reason,
                    "reason_detail": reason,
                    "pipeline_version": PIPELINE_VERSION,
                    "ingestion_timestamp": ingestion_timestamp,
                    "original_event": event,
                },
            )
            return

        event["timestamp"] = datetime.fromisoformat(
            event["timestamp"].replace("Z", "+00:00")
        )

        event["ingestion_timestamp"] = ingestion_timestamp
        event["pipeline_version"] = PIPELINE_VERSION
        event["source_ground_station"] = event.get("ground_station_id")

        yield event


class Deduplicate(beam.DoFn):
    SEEN = ReadModifyWriteStateSpec(
        name="seen",
        coder=beam.coders.BooleanCoder(),
    )

    EXPIRY = TimerSpec(
        name="expiry",
        time_domain=beam.TimeDomain.REAL_TIME,
    )

    def process(
        self,
        element,
        seen=beam.DoFn.StateParam(SEEN),
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):
        event_id, event = element

        if seen.read():
            return

        seen.write(True)

        expiry.set(Timestamp.now() + (24 * 60 * 60))

        yield event

    @on_timer(EXPIRY)
    def clear_seen(
        self,
        seen=beam.DoFn.StateParam(SEEN),
    ):
        seen.clear()


def to_raw_bigquery_row(message):
    return {
        "message_id": message.message_id,
        "publish_timestamp": to_beam_timestamp(message.publish_time),
        "ingestion_timestamp": to_beam_timestamp(Timestamp.now()),
        "payload": message.data.decode("utf-8", errors="replace"),
    }


def normalize_timestamp(value):
    if value is None:
        return None

    if isinstance(value, Timestamp):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return to_beam_timestamp(value)

    return value


def to_bigquery_row(event):
    row = dict(event)

    row["timestamp"] = to_beam_timestamp(row.get("timestamp"))
    row["ingestion_timestamp"] = to_beam_timestamp(row.get("ingestion_timestamp"))

    if row.get("telemetry") is not None:
        row["telemetry"] = json.dumps(
            row["telemetry"],
            separators=(",", ":"),
        )

    if row.get("position") is not None:
        row["position"] = json.dumps(
            row["position"],
            separators=(",", ":"),
        )

    return row


def to_dlq_message(event):
    event = dict(event)

    for field in ("timestamp", "ingestion_timestamp"):
        value = event.get(field)

        if value is None:
            continue

        if isinstance(value, Timestamp):
            event[field] = value.to_rfc3339()
        elif isinstance(value, (datetime, date)):
            event[field] = value.isoformat()
        else:
            event[field] = str(value)

    return json.dumps(
        event,
        separators=(",", ":"),
    ).encode("utf-8")


def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True,
        setup_file="./setup.py",
        max_cache_memory_usage_mb=512,
        runner=os.getenv("RUNNER", "DirectRunner"),
        project=PROJECT_ID,
        region=os.getenv("REGION"),
        service_account_email=os.getenv("SERVICE_ACCOUNT_EMAIL"),
        staging_location=os.getenv("STAGING_LOCATION"),
        temp_location=os.getenv("TEMP_LOCATION"),
    )

    with beam.Pipeline(options=options) as pipeline:
        messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=INPUT_SUBSCRIPTION,
            with_attributes=True,
        )

        raw_events = messages | "ToRawBigQueryRow" >> beam.Map(to_raw_bigquery_row)

        (
            raw_events
            | "WriteRawBigQuery"
            >> beam.io.WriteToBigQuery(
                RAW_BIGQUERY_TABLE,
                schema=RAW_BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=get_bq_write_method(),
            )
        )

        results = messages | "Validate" >> beam.ParDo(ParseAndValidate()).with_outputs(
            ParseAndValidate.INVALID,
            main=ParseAndValidate.VALID,
        )

        valid_events = results[ParseAndValidate.VALID]

        invalid_events = results[ParseAndValidate.INVALID]

        # Invalid events go to the DLQ.
        (
            invalid_events
            | "SerializeDLQ" >> beam.Map(to_dlq_message)
            | "WriteDLQ"
            >> beam.io.WriteToPubSub(
                DLQ_TOPIC,
            )
        )

        keyed_events = valid_events | "KeyByEventId" >> beam.Map(
            lambda event: (event["event_id"], event)
        ).with_output_types(Tuple[str, dict])

        deduped_events = keyed_events | "Deduplicate" >> beam.ParDo(Deduplicate())

        curated_events = deduped_events | "ToBigQueryRow" >> beam.Map(to_bigquery_row)

        (
            curated_events
            | "WriteBigQuery"
            >> beam.io.WriteToBigQuery(
                BIGQUERY_TABLE,
                schema=BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=get_bq_write_method(),
            )
        )


if __name__ == "__main__":
    run()
