# Design Decision: Late Data vs. Malformed Data and Post-Dropout Events

## Decision

The telemetry pipeline will distinguish **malformed events** from **late but valid events**. Lateness and satellite dropout status will not, by themselves, cause an event to be rejected.

## Malformed Events

An event is considered malformed when it fails the defined schema or plausibility rules, such as:

- Invalid JSON
- Missing required fields
- Incorrect field types
- Invalid sequence numbers
- Physically implausible telemetry values
- Invalid timestamps, including timestamps outside the permitted future-time tolerance

Malformed events are **not written to the curated BigQuery table**.

Instead, they are routed to the dead-letter topic with:

- `reason_code`
- `reason_detail`
- `ingestion_timestamp`
- `pipeline_version`
- Original event payload

This ensures malformed data remains inspectable and recoverable.

## Late but Valid Events

An event is considered **late** when its event timestamp is older than the current processing time but the event otherwise satisfies all validation rules.

Late events are **accepted into the curated dataset**.

The pipeline preserves:

- `timestamp` — when the telemetry event occurred
- `ingestion_timestamp` — when the pipeline processed the event

This allows downstream consumers to identify and analyze late-arriving telemetry without treating it as corrupt data.

## Events Arriving After a Dropout Window

Satellite dropout detection is treated as a **monitoring and observability concern**, not an event-validation rule.

If a satellite is detected as having gone silent and valid telemetry subsequently arrives after the dropout window closes:

1. The event is validated normally.
2. If valid, it is accepted into the curated dataset.
3. Its original event timestamp is preserved.
4. Its ingestion timestamp is recorded.
5. Duplicate `event_id` values are suppressed.
6. Downstream monitoring can use the event to determine when the satellite recovered and whether the telemetry arrived late.

A dropout therefore does **not** create a permanent rejection window.

## Rationale

This separation prevents three different conditions from being incorrectly treated as the same problem:

| Condition                 | Treatment                   |
| ------------------------- | --------------------------- |
| Malformed event           | Reject and route to DLQ     |
| Late but valid event      | Accept into curated dataset |
| Valid event after dropout | Accept into curated dataset |
| Duplicate `event_id`      | Suppress duplicate delivery |

This preserves valid telemetry while maintaining a clear audit trail for malformed data and allowing downstream systems to independently analyze:

- Lateness
- Missing telemetry
- Dropout duration
- Satellite recovery
- Sequence gaps

## Summary

The pipeline separates **data validity**, **event timing**, and **satellite availability**.

> **Malformed data is rejected and routed to the DLQ. Late but valid data is accepted. Valid telemetry received after a satellite dropout is also accepted. Dropout status does not determine event validity.**

This policy ensures that operational events such as delayed delivery, satellite silence, and recovery do not result in the loss of otherwise valid telemetry.

**Status:** Implemented policy for the streaming pipeline.
