provider "snowflake" {
  # For PAT authentication, use 'account' parameter
  # Format: "account_locator" or "organization_name-account_name" or "account_locator.region.cloud"
  account       = var.snowflake_account
  user          = var.snowflake_user
  authenticator = "PROGRAMMATIC_ACCESS_TOKEN"  # Required for PAT authentication
  token         = var.snowflake_oauth_access_token  # PAT token
  warehouse     = var.snowflake_warehouse
  role          = var.snowflake_role
  
  # Enable preview features for semantic views
  preview_features_enabled = ["snowflake_semantic_view_resource"]
}

variable "snowflake_account" {
  description = "Snowflake account identifier. Can be: account_locator, organization_name-account_name, or account_locator.region.cloud"
  type        = string
  default     = ""
}

variable "snowflake_user" {
  description = "Snowflake user"
  type        = string
  default     = ""
}

variable "snowflake_oauth_access_token" {
  description = "Snowflake Personal Access Token (PAT) - OAuth access token. Get from: Snowflake Web UI → User Profile → Personal Access Tokens"
  type        = string
  sensitive   = true
  default     = ""
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse"
  type        = string
  default     = ""
}

variable "snowflake_database" {
  description = "Snowflake database"
  type        = string
  default     = "YOUR_DATABASE"
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
  default     = ""
}

