output "access_key" {
  sensitive = true
  value     = yandex_iam_service_account_static_access_key.sa_static_key.access_key
}

output "secret_key" {
  sensitive = true
  value     = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
}