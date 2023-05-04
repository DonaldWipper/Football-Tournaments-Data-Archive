locals {
  data_lake_bucket = "euro_stat"
}

variable "folder_id" {
  description = "The ID of the folder to operate under"
  type        = string
}

variable "cloud_id" {
  description = "The ID of the cloud to operate under"
  type        = string
}

variable "auth_token" {
  description = "Security token or IAM token used for authentication in Yandex.Cloud"
  type        = string
}

variable "zone" {
  description = "Availability zone"
  type        = string
  default     = "ru-central1-a"
}