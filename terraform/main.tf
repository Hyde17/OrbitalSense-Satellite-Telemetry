# ------------------------------------------------------------
# APIs
# ------------------------------------------------------------

resource "google_project_service" "services" {
  for_each = toset([
    "run.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}


# ------------------------------------------------------------
# IAM Service Accounts
# ------------------------------------------------------------

resource "google_service_account" "cloud_run" {
  account_id   = "cloud-run-${var.environment}"
  display_name = "Cloud Run ${var.environment} service account"

  depends_on = [
    google_project_service.services
  ]
}

resource "google_service_account" "pipeline" {
  account_id   = "pipeline-${var.environment}"
  display_name = "Pipeline ${var.environment} service account"

  depends_on = [
    google_project_service.services
  ]
}


# ------------------------------------------------------------
# IAM Roles
# ------------------------------------------------------------

resource "google_project_iam_member" "cloud_run_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "pipeline_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}


# ------------------------------------------------------------
# Cloud Storage
# ------------------------------------------------------------

resource "google_storage_bucket" "data" {
  name                        = var.bucket_name
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type = "Delete"
    }
  }

  depends_on = [
    google_project_service.services
  ]
}


# ------------------------------------------------------------
# Pub/Sub
# ------------------------------------------------------------
resource "google_pubsub_topic" "events_dead_letter" {
  name    = "events-dead-letter-${var.environment}"
  project = var.project_id

  depends_on = [
    google_project_service.services
  ]
}

resource "google_pubsub_topic" "events" {
  name    = "events-${var.environment}"
  project = var.project_id

  depends_on = [
    google_project_service.services
  ]
}

resource "google_pubsub_subscription" "events" {
  name    = "events-${var.environment}-subscription"
  topic   = google_pubsub_topic.events.id
  project = var.project_id

  ack_deadline_seconds = 30

  message_retention_duration = "604800s"
}


# ------------------------------------------------------------
# BigQuery
# ------------------------------------------------------------

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics_${var.environment}"
  project    = var.project_id
  location   = var.region

  delete_contents_on_destroy = false

  depends_on = [
    google_project_service.services
  ]
}

resource "google_bigquery_table" "events" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "events"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "payload"
      type = "JSON"
      mode = "NULLABLE"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "telemetry" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "telemetry"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "satellite_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ground_station_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "sequence_number"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "subsystem"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "telemetry"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "position"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "pipeline_version"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "source_ground_station"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])
}

# ------------------------------------------------------------
# Artifact Registry
# ------------------------------------------------------------

resource "google_artifact_registry_repository" "docker" {
  location      = var.region
  repository_id = "containers-${var.environment}"
  description   = "Docker container repository"
  format        = "DOCKER"
  project       = var.project_id

  depends_on = [
    google_project_service.services
  ]
}


# ------------------------------------------------------------
# Cloud Run
# ------------------------------------------------------------




# ------------------------------------------------------------
# Cloud Run IAM
# ------------------------------------------------------------

