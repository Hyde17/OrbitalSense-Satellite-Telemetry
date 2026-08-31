output "storage_bucket" {
  description = "Cloud Storage bucket"
  value       = google_storage_bucket.data.name
}

output "pubsub_topic" {
  description = "Pub/Sub topic"
  value       = google_pubsub_topic.events.name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription"
  value       = google_pubsub_subscription.events.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset"
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository"
  value       = google_artifact_registry_repository.docker.name
}


output "cloud_run_service_account" {
  value = google_service_account.cloud_run.email
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}