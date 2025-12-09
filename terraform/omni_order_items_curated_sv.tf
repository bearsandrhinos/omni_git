resource "snowflake_semantic_view" "omni_order_items_curated_sv" {
  database = var.snowflake_database
  schema   = "ECOMM"
  name     = "omni_order_items_curated_sv"
  comment  = "Detail on historical customer purchases"


  lifecycle {
    create_before_destroy = true
  }

  tables {
    table_alias = "ECOMM_ORDER_ITEMS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"ORDER_ITEMS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "ECOMM_USERS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"USERS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "ECOMM_INVENTORY_ITEMS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"INVENTORY_ITEMS\""
    primary_key = ["ID"]
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.id"
    sql_expression            = "ECOMM_ORDER_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.order_id"
    sql_expression            = "ECOMM_ORDER_ITEMS.ORDER_ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.user_id"
    sql_expression            = "ECOMM_ORDER_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.inventory_item_id"
    sql_expression            = "ECOMM_ORDER_ITEMS.INVENTORY_ITEM_ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.status"
    sql_expression            = "ECOMM_ORDER_ITEMS.STATUS"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.created_at"
    sql_expression            = "ECOMM_ORDER_ITEMS.CREATED_AT"
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
    qualified_expression_name = "ECOMM_USERS.created_at"
    sql_expression            = "ECOMM_USERS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_USERS.domain_from_email"
    sql_expression            = "SUBSTRING(ECOMM_USERS.EMAIL, COALESCE(CASE WHEN POSITION('@', SUBSTRING(ECOMM_USERS.EMAIL, 1, LENGTH(ECOMM_USERS.EMAIL))) > 0 THEN POSITION('@', SUBSTRING(ECOMM_USERS.EMAIL, 1, LENGTH(ECOMM_USERS.EMAIL))) + (1 - 1) ELSE NULL END, 0) + 1, LENGTH(ECOMM_USERS.EMAIL))"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.id"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.cost"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.COST"
  }

  facts {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.sale_price"
    sql_expression            = "ECOMM_ORDER_ITEMS.SALE_PRICE"
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_ORDER_ITEMS.count"
      sql_expression            = "COUNT(DISTINCT ECOMM_ORDER_ITEMS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_ORDER_ITEMS.created_at_min"
      sql_expression            = "MIN(ECOMM_ORDER_ITEMS.CREATED_AT)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_ORDER_ITEMS.total_sale_price"
      sql_expression            = "SUM(ECOMM_ORDER_ITEMS.SALE_PRICE * 100)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_USERS.age_average"
      sql_expression            = "AVG(ECOMM_USERS.AGE)"
      comment                   = "The average age of a customer"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.cost_sum"
      sql_expression            = "SUM(ECOMM_INVENTORY_ITEMS.COST)"
    }
  }

}
