resource "snowflake_semantic_view" "omni_users_sv" {
  database = var.snowflake_database
  schema   = "ECOMM"
  name     = "omni_users_sv"
  comment  = "All registered users"

  tables {
    table_alias = "ECOMM_USERS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"USERS\""
    primary_key = ["ID"]
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.full_name"
    sql_expression            = "INITCAP(CONCAT(ECOMM_USERS.FIRST_NAME, ' ', ECOMM_USERS.LAST_NAME))"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.id"
    sql_expression            = "ECOMM_USERS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.first_name"
    sql_expression            = "ECOMM_USERS.FIRST_NAME"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.last_name"
    sql_expression            = "ECOMM_USERS.LAST_NAME"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.email"
    sql_expression            = "ECOMM_USERS.EMAIL"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.age"
    sql_expression            = "ECOMM_USERS.AGE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.city"
    sql_expression            = "ECOMM_USERS.CITY"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.state"
    sql_expression            = "ECOMM_USERS.STATE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.zip"
    sql_expression            = "ECOMM_USERS.ZIP"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.latitude"
    sql_expression            = "ECOMM_USERS.LATITUDE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.longitude"
    sql_expression            = "ECOMM_USERS.LONGITUDE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.gender"
    sql_expression            = "ECOMM_USERS.GENDER"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.created_at"
    sql_expression            = "ECOMM_USERS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.traffic_source"
    sql_expression            = "ECOMM_USERS.TRAFFIC_SOURCE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.domain_from_email"
    sql_expression            = "SUBSTRING(ECOMM_USERS.EMAIL, COALESCE(CASE WHEN POSITION('@', SUBSTRING(ECOMM_USERS.EMAIL, 1, LENGTH(ECOMM_USERS.EMAIL))) > 0 THEN POSITION('@', SUBSTRING(ECOMM_USERS.EMAIL, 1, LENGTH(ECOMM_USERS.EMAIL))) + (1 - 1) ELSE NULL END, 0) + 1, LENGTH(ECOMM_USERS.EMAIL))"
  }

  facts {
    qualified_expression_name = "ECOMM_USERS.country"
    sql_expression            = "ECOMM_USERS.COUNTRY"
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_USERS.count"
      sql_expression            = "COUNT(DISTINCT ECOMM_USERS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_USERS.age_average"
      sql_expression            = "AVG(ECOMM_USERS.AGE)"
      comment                   = "The average age of a customer"
    }
  }

}
