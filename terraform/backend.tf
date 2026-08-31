terraform {
  backend "gcs" {
    bucket = "orbitalsense-2026-terraform-state"
    prefix = "terraform/state"
  }
}