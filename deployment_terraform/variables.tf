variable "project_id" {
    description = "Identifier of the GCP project to deploy to"
    type = string
}

variable "region" {
    description = "Region of deployment"
    type = string
    default = "us-central1"
}

variable "sa_name" {
    description = "Service account to use"
    type = string  
}

variable "repo_id" {
    description = "Identifier of Artifact Registry repository"
    type = string
}