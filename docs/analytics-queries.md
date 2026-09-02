# Analytics Queries

The following BigQuery queries support telemetry analysis for OrbitalSense.
Telemetry measurements are stored as JSON strings in the `telemetry` column.
The alert queries use explicit operational thresholds because the current
schema does not contain an `alert` column.

## 1. Which satellites generated the most telemetry?

```sql
SELECT
  satellite_id,
  COALESCE(source_ground_station, ground_station_id) AS ground_station_id,
  COUNT(*) AS telemetry_count,
  MIN(timestamp) AS first_event,
  MAX(timestamp) AS last_event
FROM `orbitalsense-2026.analytics_dev.telemetry`
GROUP BY
  satellite_id,
  ground_station_id
ORDER BY
  satellite_id,
  telemetry_count DESC;
```

## 2. Average battery voltage and failure trend

```sql
WITH battery AS (
  SELECT
    satellite_id,
    timestamp,
    SAFE_CAST(JSON_VALUE(telemetry, '$.voltage_v') AS FLOAT64) AS battery_voltage
  FROM `orbitalsense-2026.analytics_dev.telemetry`
  WHERE subsystem = 'power'
),

trend AS (
  SELECT
    satellite_id,
    timestamp,
    battery_voltage,
    LAG(battery_voltage) OVER (
      PARTITION BY satellite_id
      ORDER BY timestamp
    ) AS previous_voltage
  FROM battery
)

SELECT
  satellite_id,
  ROUND(AVG(battery_voltage), 3) AS average_battery_voltage,
  ROUND(MIN(battery_voltage), 3) AS minimum_battery_voltage,
  ROUND(MAX(battery_voltage), 3) AS maximum_battery_voltage,
  ROUND(AVG(battery_voltage - previous_voltage), 4) AS average_voltage_change,
  COUNT(*) AS observations
FROM trend
GROUP BY satellite_id
ORDER BY average_voltage_change ASC;
```

## 3. Satellites with the weakest communication signal

```sql
SELECT
  satellite_id,
  ROUND(AVG(signal_dbm), 2) AS avg_signal_dbm,
  ROUND(MIN(signal_dbm), 2) AS weakest_signal_dbm,
  ROUND(MAX(signal_dbm), 2) AS strongest_signal_dbm,
  COUNT(*) AS observations,
  MIN(timestamp) AS window_start,
  MAX(timestamp) AS window_end
FROM (
  SELECT
    satellite_id,
    timestamp,
    SAFE_CAST(JSON_VALUE(telemetry, '$.signal_dbm') AS FLOAT64) AS signal_dbm
  FROM `orbitalsense-2026.analytics_dev.telemetry`
  WHERE subsystem = 'communications'
)
WHERE signal_dbm IS NOT NULL
  AND timestamp >= TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL 24 HOUR
  )
GROUP BY satellite_id
ORDER BY avg_signal_dbm ASC;
```

## 4. Which subsystem generated the most threshold alerts?

The current producer does not emit an alert flag. This query counts readings
that cross these thresholds: power voltage below 24 V or battery below 20%,
thermal temperature below 0 C or above 70 C, and communications signal below
-90 dBm or packet loss above 5%.

```sql
SELECT
  subsystem,
  COUNT(*) AS threshold_alert_count,
  MIN(timestamp) AS first_alert,
  MAX(timestamp) AS last_alert
FROM (
  SELECT
    subsystem,
    timestamp,
    SAFE_CAST(JSON_VALUE(telemetry, '$.voltage_v') AS FLOAT64) AS voltage_v,
    SAFE_CAST(JSON_VALUE(telemetry, '$.battery_percent') AS FLOAT64) AS battery_percent,
    SAFE_CAST(JSON_VALUE(telemetry, '$.temperature_c') AS FLOAT64) AS temperature_c,
    SAFE_CAST(JSON_VALUE(telemetry, '$.signal_dbm') AS FLOAT64) AS signal_dbm,
    SAFE_CAST(JSON_VALUE(telemetry, '$.packet_loss_percent') AS FLOAT64) AS packet_loss_percent
  FROM `orbitalsense-2026.analytics_dev.telemetry`
)
WHERE (subsystem = 'power' AND (voltage_v < 24 OR battery_percent < 20))
   OR (subsystem = 'thermal' AND (temperature_c < 0 OR temperature_c > 70))
   OR (subsystem = 'communications' AND (signal_dbm < -90 OR packet_loss_percent > 5))
GROUP BY subsystem
ORDER BY threshold_alert_count DESC;
```

## 5. Compare threshold alerts with malformed raw events

Raw events do not have validation-status columns. This query identifies
quarantined events from the original payload by checking required fields,
timestamp, sequence number, and telemetry object shape.

```sql
WITH alerts AS (
  SELECT
    subsystem,
    COUNT(*) AS threshold_alert_count
  FROM (
    SELECT
      subsystem,
      timestamp,
      SAFE_CAST(JSON_VALUE(telemetry, '$.voltage_v') AS FLOAT64) AS voltage_v,
      SAFE_CAST(JSON_VALUE(telemetry, '$.battery_percent') AS FLOAT64) AS battery_percent,
      SAFE_CAST(JSON_VALUE(telemetry, '$.temperature_c') AS FLOAT64) AS temperature_c,
      SAFE_CAST(JSON_VALUE(telemetry, '$.signal_dbm') AS FLOAT64) AS signal_dbm,
      SAFE_CAST(JSON_VALUE(telemetry, '$.packet_loss_percent') AS FLOAT64) AS packet_loss_percent
    FROM `orbitalsense-2026.analytics_dev.telemetry`
  )
  WHERE timestamp >= TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL 24 HOUR
    )
    AND ((subsystem = 'power' AND (voltage_v < 24 OR battery_percent < 20))
      OR (subsystem = 'thermal' AND (temperature_c < 0 OR temperature_c > 70))
      OR (subsystem = 'communications' AND (signal_dbm < -90 OR packet_loss_percent > 5)))
  GROUP BY subsystem
),

quarantine AS (
  SELECT
    JSON_VALUE(payload, '$.subsystem') AS subsystem,
    COUNT(*) AS quarantined_count
  FROM `orbitalsense-2026.analytics_dev.telemetry_raw`
  WHERE ingestion_timestamp >= TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL 24 HOUR
    )
    AND (JSON_VALUE(payload, '$.event_id') IS NULL
      OR JSON_VALUE(payload, '$.satellite_id') IS NULL
      OR SAFE_CAST(JSON_VALUE(payload, '$.sequence_number') AS INT64) IS NULL
      OR SAFE_CAST(JSON_VALUE(payload, '$.timestamp') AS TIMESTAMP) IS NULL
      OR JSON_QUERY(payload, '$.telemetry') IS NULL)
  GROUP BY subsystem
)

SELECT
  COALESCE(a.subsystem, q.subsystem) AS subsystem,
  COALESCE(a.threshold_alert_count, 0) AS threshold_alert_count,
  COALESCE(q.quarantined_count, 0) AS quarantined_count
FROM alerts a
FULL OUTER JOIN quarantine q
  ON a.subsystem = q.subsystem
ORDER BY threshold_alert_count DESC;
```