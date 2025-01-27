variable "bq_dataset_name" {
  description = "My Biq Query Dataset Name"
  default     = "DE_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "hazel-planet-448917-f5-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "location" {
  description = "My Project location"
  default     = "EU"
}

variable "project" {
  description = "My Project Name"
  default     = "hazel-planet-448917-f5"
}

variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}