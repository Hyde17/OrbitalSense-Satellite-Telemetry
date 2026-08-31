# app/consumer/validate.py

from datetime import datetime


REQUIRED_FIELDS = {
    "event_id",
    "satellite_id",
    "sequence_number",
    "timestamp",
    "subsystem",
}


def validate_event(event):
    """
    Validate a telemetry event.

    Returns:
        (True, None) when valid.
        (False, reason_code) when invalid.
    """

    if not isinstance(event, dict):
        return False, "INVALID_EVENT_FORMAT"

    missing_fields = REQUIRED_FIELDS - event.keys()

    if missing_fields:
        return False, "MISSING_REQUIRED_FIELDS"

    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        return False, "INVALID_EVENT_ID"

    if not isinstance(event["satellite_id"], str) or not event["satellite_id"].strip():
        return False, "INVALID_SATELLITE_ID"

    if not isinstance(event["sequence_number"], int):
        return False, "INVALID_SEQUENCE_NUMBER"

    if not isinstance(event["timestamp"], str):
        return False, "INVALID_TIMESTAMP"

    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False, "INVALID_TIMESTAMP"

    if not isinstance(event["subsystem"], str) or not event["subsystem"].strip():
        return False, "INVALID_SUBSYSTEM"

    return True, None
