provider "snowflake" {
  # For provider v2.0+, both account_name and organization_name are required
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "PROGRAMMATIC_ACCESS_TOKEN"  # Required for PAT authentication
  token             = var.snowflake_oauth_access_token  # PAT token
  warehouse         = var.snowflake_warehouse
  role              = var.snowflake_role
  
  # Enable preview features for semantic views
  preview_features_enabled = ["snowflake_semantic_view_resource"]
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name (required for provider v2.0+)"
  type        = string
  default     = ""
}

variable "snowflake_account_name" {
  description = "Snowflake account name (required for provider v2.0+)"
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

