from datetime import datetime, timezone
from apache_beam.utils.timestamp import Timestamp


def parse_timestamp(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_beam_timestamp(value):
    if value is None:
        return None

    if isinstance(value, Timestamp):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return Timestamp.from_utc_datetime(value)

    raise TypeError(
        f"Expected datetime or Beam Timestamp, got {type(value)!r}: {value!r}"
    )
