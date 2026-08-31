variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
  default     = "europe-west1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Globally unique Cloud Storage bucket name"
  type        = string
}

variable "cloud_run_image" {
  description = "Container image for Cloud Run"
  type        = string
}