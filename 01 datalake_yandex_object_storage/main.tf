provider "yandex" {
  token     = var.auth_token
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zone
}

# Create SA
resource "yandex_iam_service_account" "sa" {
  name      = "storage-sa"
  folder_id = var.folder_id
}

# Grant permissions
resource "yandex_resourcemanager_folder_iam_member" "sa_editor" {
  folder_id = var.folder_id
  role      = "storage.editor"
  member    = "serviceAccountx:${yandex_iam_service_account.sa.id}"
}

# Create Static Access Keys
resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}

# Use keys to create bucket
resource "yandex_storage_bucket" "storage_bucket" {
  access_key    = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.sa_static_key.secretx_key
  bucket_prefix = "storage-bucket-"
  force_destroy = true
}