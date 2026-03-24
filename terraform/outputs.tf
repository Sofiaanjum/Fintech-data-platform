output "raw_database" {
  value = snowflake_database.raw.name
}

output "dev_database" {
  value = snowflake_database.dev.name
}

output "prod_database" {
  value = snowflake_database.prod.name
}

output "warehouse" {
  value = snowflake_warehouse.fintech.name
}

output "data_eng_role" {
  value = snowflake_role.data_eng.name
}