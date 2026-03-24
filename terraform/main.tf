terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_user
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

# ── Databases ────────────────────────────────────────────────────────────────

resource "snowflake_database" "raw" {
  name    = "FINTECH_RAW_DB"
  comment = "Raw landing zone -- data arrives here from Kafka consumers"
}

resource "snowflake_database" "dev" {
  name    = "FINTECH_DEV_DB"
  comment = "Development environment -- dbt dev target"
}

resource "snowflake_database" "prod" {
  name    = "FINTECH_PROD_DB"
  comment = "Production environment -- dbt prod target"
}

# ── Schemas ──────────────────────────────────────────────────────────────────

resource "snowflake_schema" "raw_transactions" {
  database = snowflake_database.raw.name
  name     = "TRANSACTIONS"
}

resource "snowflake_schema" "raw_audit" {
  database = snowflake_database.raw.name
  name     = "AUDIT"
}

resource "snowflake_schema" "dev_staging" {
  database = snowflake_database.dev.name
  name     = "STAGING"
}

resource "snowflake_schema" "dev_intermediate" {
  database = snowflake_database.dev.name
  name     = "INTERMEDIATE"
}

resource "snowflake_schema" "dev_marts" {
  database = snowflake_database.dev.name
  name     = "MARTS"
}

resource "snowflake_schema" "prod_staging" {
  database = snowflake_database.prod.name
  name     = "STAGING"
}

resource "snowflake_schema" "prod_intermediate" {
  database = snowflake_database.prod.name
  name     = "INTERMEDIATE"
}

resource "snowflake_schema" "prod_marts" {
  database = snowflake_database.prod.name
  name     = "MARTS"
}

# ── Warehouse ────────────────────────────────────────────────────────────────

resource "snowflake_warehouse" "fintech" {
  name           = "FINTECH_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Primary warehouse for fintech pipeline"
}

# ── Roles ────────────────────────────────────────────────────────────────────

resource "snowflake_role" "data_eng" {
  name    = "DATA_ENG_ROLE"
  comment = "Full access for data engineering team"
}

resource "snowflake_role" "analyst" {
  name    = "ANALYST_ROLE"
  comment = "Read-only access to prod marts"
}

# ── Grants ───────────────────────────────────────────────────────────────────

resource "snowflake_grant_privileges_to_role" "eng_raw_db" {
  role_name  = snowflake_role.data_eng.name
  privileges = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_role" "eng_dev_db" {
  role_name  = snowflake_role.data_eng.name
  privileges = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dev.name
  }
}

resource "snowflake_grant_privileges_to_role" "eng_prod_db" {
  role_name  = snowflake_role.data_eng.name
  privileges = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_role" "analyst_prod_db" {
  role_name  = snowflake_role.analyst.name
  privileges = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.prod.name
  }
}

resource "snowflake_grant_privileges_to_role" "eng_warehouse" {
  role_name  = snowflake_role.data_eng.name
  privileges = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.fintech.name
  }
}