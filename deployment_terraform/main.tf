# Enable Cloud Run API service
resource "google_project_service" "services" {
  project   = var.project_id
  service   = "run.googleapis.com"
}

# Create a new service account
resource "google_service_account" "nwmapi2_service_account" {
    account_id   = var.sa_name
    display_name = "NWM API V2 SA"
}

# List different roles to be assigned
locals {
  service_account_roles = [
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser",
    "roles/storage.objectViewer",
    "roles/run.invoker"
  ]
}

# Assign the listed roles to the created service account
resource "google_project_iam_binding" "bq_viewer_account_iam" {
    project = var.project_id
    for_each = toset(local.service_account_roles)
    role    = each.key
    members = [
      "serviceAccount:${google_service_account.nwmapi2_service_account.email}",
    ]
}

# Create repository in the Artifact Registry
resource "google_artifact_registry_repository" "nwm-api-repo" {
  location      = var.region
  repository_id = var.repo_id
  description   = "Repository for the NWM API V2 Docker Image"
  format        = "DOCKER"
}

