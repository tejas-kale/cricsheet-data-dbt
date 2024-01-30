locals {
  data_lake_bucket = "cricsheet_data_lake"
}

variable "project" {
  description = "GCP project ID"
  default = "august-cirrus-399913"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west4"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "cricsheet_raw"
}
