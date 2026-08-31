import json
import math
import random
import time
import uuid
from datetime import datetime, timezone


GROUND_STATIONS = [
    {
        "id": "gs-01",
        "latitude": 6.5244,
        "longitude": 3.3792,
        "coverage_km": 2500,
    },
    {
        "id": "gs-02",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "coverage_km": 2500,
    },
    {
        "id": "gs-03",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "coverage_km": 2500,
    },
    {
        "id": "gs-04",
        "latitude": -33.8688,
        "longitude": 151.2093,
        "coverage_km": 2500,
    },
]

SATELLITES = [
    {
        "id": f"sat-{i:02d}",
        "latitude": random.uniform(-70, 70),
        "longitude": random.uniform(-180, 180),
        "altitude_km": random.uniform(450, 800),
    }
    for i in range(1, 13)
]

SEQUENCE_NUMBERS = {sat["id"]: 0 for sat in SATELLITES}

# sat-03 will intentionally stop sending and then resume.
SILENT_SATELLITE = "sat-03"
SILENT_START_SECONDS = 120
SILENT_DURATION_SECONDS = 60

START_TIME = time.monotonic()


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points on Earth."""

    earth_radius_km = 6371.0

    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )

    return 2 * earth_radius_km * math.asin(math.sqrt(a))


def find_ground_station(satellite):
    """Return the nearest ground station if the satellite is in range."""

    candidates = []

    for station in GROUND_STATIONS:
        distance = haversine_km(
            satellite["latitude"],
            satellite["longitude"],
            station["latitude"],
            station["longitude"],
        )

        if distance <= station["coverage_km"]:
            candidates.append((distance, station["id"]))

    if not candidates:
        return None

    candidates.sort()
    return candidates[0][1]


def update_orbit(satellite):
    """Move the satellite slightly to simulate orbital movement."""

    satellite["latitude"] += random.uniform(-1.5, 1.5)
    satellite["longitude"] += random.uniform(-3.0, 3.0)

    if satellite["latitude"] > 90:
        satellite["latitude"] = 90

    if satellite["latitude"] < -90:
        satellite["latitude"] = -90

    if satellite["longitude"] > 180:
        satellite["longitude"] -= 360

    if satellite["longitude"] < -180:
        satellite["longitude"] += 360


def power_reading():
    return {
        "voltage_v": round(random.gauss(28.0, 0.8), 2),
        "current_a": round(random.gauss(4.5, 0.5), 2),
        "battery_percent": round(
            max(5, min(100, random.gauss(82, 8))),
            2,
        ),
    }


def thermal_reading():
    return {
        "temperature_c": round(random.gauss(35, 6), 2),
        "heater_active": random.random() < 0.15,
    }


def communications_reading():
    return {
        "signal_dbm": round(random.gauss(-70, 8), 2),
        "packet_loss_percent": round(
            max(0, min(100, random.gauss(1.5, 1.0))),
            2,
        ),
    }


def orbital_reading(satellite):
    return {
        "latitude": round(satellite["latitude"], 5),
        "longitude": round(satellite["longitude"], 5),
        "altitude_km": round(satellite["altitude_km"], 2),
    }


def create_event(satellite, subsystem):
    satellite_id = satellite["id"]

    SEQUENCE_NUMBERS[satellite_id] += 1
    sequence = SEQUENCE_NUMBERS[satellite_id]

    ground_station = find_ground_station(satellite)

    event = {
        "event_id": str(uuid.uuid4()),
        "satellite_id": satellite_id,
        "ground_station_id": ground_station,
        "sequence_number": sequence,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "subsystem": subsystem,
        "telemetry": {},
        "position": orbital_reading(satellite),
    }

    if subsystem == "power":
        event["telemetry"] = power_reading()

    elif subsystem == "thermal":
        event["telemetry"] = thermal_reading()

    elif subsystem == "communications":
        event["telemetry"] = communications_reading()

    elif subsystem == "orbital":
        event["telemetry"] = orbital_reading(satellite)

    return event


def make_malformed(event):
    """Intentionally corrupt a valid telemetry event."""

    malformed = dict(event)

    corruption = random.choice(
        [
            "missing_satellite",
            "invalid_sequence",
            "invalid_telemetry",
            "invalid_timestamp",
        ]
    )

    if corruption == "missing_satellite":
        malformed.pop("satellite_id", None)

    elif corruption == "invalid_sequence":
        malformed["sequence_number"] = "INVALID"

    elif corruption == "invalid_telemetry":
        malformed["telemetry"] = "CORRUPTED"

    elif corruption == "invalid_timestamp":
        malformed["timestamp"] = "not-a-timestamp"

    return malformed


def should_be_silent(satellite_id):
    elapsed = time.monotonic() - START_TIME

    return (
        satellite_id == SILENT_SATELLITE
        and SILENT_START_SECONDS
        <= elapsed
        < SILENT_START_SECONDS + SILENT_DURATION_SECONDS
    )


def generate_events():
    """Generate one batch of telemetry events."""

    events = []

    for satellite in SATELLITES:
        if should_be_silent(satellite["id"]):
            continue

        update_orbit(satellite)

        for subsystem in [
            "power",
            "thermal",
            "communications",
            "orbital",
        ]:
            event = create_event(satellite, subsystem)

            # Approximately 2% malformed events.
            if random.random() < 0.02:
                event = make_malformed(event)

            events.append(event)

            # Approximately 3% duplicate transmissions.
            if random.random() < 0.03:
                events.append(event)

    random.shuffle(events)

    return events


def serialize(event):
    return json.dumps(event)
