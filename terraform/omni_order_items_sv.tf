resource "snowflake_semantic_view" "omni_order_items_sv" {
  database = var.snowflake_database
  schema   = "PUBLIC"
  name     = "omni_order_items_sv"
  comment  = "Detail on historical customer purchases"

  tables {
    table_alias = "ORDER_ITEMS"
    table_name  = "${var.snowflake_database}.\"PUBLIC\".\"ORDER_ITEMS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "USERS"
    table_name  = "${var.snowflake_database}.\"PUBLIC\".\"USERS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "INVENTORY_ITEMS"
    table_name  = "${var.snowflake_database}.\"PUBLIC\".\"INVENTORY_ITEMS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "PRODUCTS"
    table_name  = "${var.snowflake_database}.\"PUBLIC\".\"PRODUCTS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "DISTRIBUTION_CENTERS"
    table_name  = "${var.snowflake_database}.\"PUBLIC\".\"DISTRIBUTION_CENTERS\""
    primary_key = ["ID"]
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.id"
    sql_expression            = "ORDER_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.order_id"
    sql_expression            = "ORDER_ITEMS.ORDER_ID"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.user_id"
    sql_expression            = "ORDER_ITEMS.USER_ID"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.inventory_item_id"
    sql_expression            = "ORDER_ITEMS.INVENTORY_ITEM_ID"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.status"
    sql_expression            = "ORDER_ITEMS.STATUS"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.created_at"
    sql_expression            = "ORDER_ITEMS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.returned_at"
    sql_expression            = "ORDER_ITEMS.RETURNED_AT"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.shipped_at"
    sql_expression            = "ORDER_ITEMS.SHIPPED_AT"
  }

  dimensions {
    qualified_expression_name = "ORDER_ITEMS.delivered_at"
    sql_expression            = "ORDER_ITEMS.DELIVERED_AT"
  }

  dimensions {
    qualified_expression_name = "USERS.id"
    sql_expression            = "USERS.ID"
  }

  dimensions {
    qualified_expression_name = "USERS.first_name"
    sql_expression            = "USERS.FIRST_NAME"
  }

  dimensions {
    qualified_expression_name = "USERS.last_name"
    sql_expression            = "USERS.LAST_NAME"
  }

  dimensions {
    qualified_expression_name = "USERS.email"
    sql_expression            = "USERS.EMAIL"
  }

  dimensions {
    qualified_expression_name = "USERS.age"
    sql_expression            = "USERS.AGE"
  }

  dimensions {
    qualified_expression_name = "USERS.city"
    sql_expression            = "USERS.CITY"
  }

  dimensions {
    qualified_expression_name = "USERS.state"
    sql_expression            = "USERS.STATE"
  }

  dimensions {
    qualified_expression_name = "USERS.zip"
    sql_expression            = "USERS.ZIP"
  }

  dimensions {
    qualified_expression_name = "USERS.latitude"
    sql_expression            = "USERS.LATITUDE"
  }

  dimensions {
    qualified_expression_name = "USERS.longitude"
    sql_expression            = "USERS.LONGITUDE"
  }

  dimensions {
    qualified_expression_name = "USERS.gender"
    sql_expression            = "USERS.GENDER"
  }

  dimensions {
    qualified_expression_name = "USERS.created_at"
    sql_expression            = "USERS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "USERS.traffic_source"
    sql_expression            = "USERS.TRAFFIC_SOURCE"
  }

  dimensions {
    qualified_expression_name = "USERS.extract_domain_from_email"
    sql_expression            = "RIGHT(USERS.EMAIL, LENGTH(USERS.EMAIL) - CASE WHEN POSITION('@', SUBSTRING(USERS.EMAIL, 1, LENGTH(USERS.EMAIL))) > 0 THEN POSITION('@', SUBSTRING(USERS.EMAIL, 1, LENGTH(USERS.EMAIL))) + (1 - 1) ELSE NULL END)"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.id"
    sql_expression            = "INVENTORY_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_id"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_ID"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.created_at"
    sql_expression            = "INVENTORY_ITEMS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.sold_at"
    sql_expression            = "INVENTORY_ITEMS.SOLD_AT"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.cost"
    sql_expression            = "INVENTORY_ITEMS.COST"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_category"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_CATEGORY"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_name"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_NAME"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_brand"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_BRAND"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_department"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_DEPARTMENT"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_sku"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_SKU"
  }

  dimensions {
    qualified_expression_name = "INVENTORY_ITEMS.product_distribution_center_id"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_DISTRIBUTION_CENTER_ID"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.id"
    sql_expression            = "PRODUCTS.ID"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.cost"
    sql_expression            = "PRODUCTS.COST"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.category"
    sql_expression            = "PRODUCTS.CATEGORY"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.name"
    sql_expression            = "PRODUCTS.NAME"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.brand"
    sql_expression            = "PRODUCTS.BRAND"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.department"
    sql_expression            = "PRODUCTS.DEPARTMENT"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.sku"
    sql_expression            = "PRODUCTS.SKU"
  }

  dimensions {
    qualified_expression_name = "PRODUCTS.distribution_center_id"
    sql_expression            = "PRODUCTS.DISTRIBUTION_CENTER_ID"
  }

  dimensions {
    qualified_expression_name = "DISTRIBUTION_CENTERS.id"
    sql_expression            = "DISTRIBUTION_CENTERS.ID"
  }

  dimensions {
    qualified_expression_name = "DISTRIBUTION_CENTERS.name"
    sql_expression            = "DISTRIBUTION_CENTERS.NAME"
  }

  dimensions {
    qualified_expression_name = "DISTRIBUTION_CENTERS.latitude"
    sql_expression            = "DISTRIBUTION_CENTERS.LATITUDE"
  }

  dimensions {
    qualified_expression_name = "DISTRIBUTION_CENTERS.longitude"
    sql_expression            = "DISTRIBUTION_CENTERS.LONGITUDE"
  }

  facts {
    qualified_expression_name = "ORDER_ITEMS.sale_price"
    sql_expression            = "ORDER_ITEMS.SALE_PRICE"
  }

  facts {
    qualified_expression_name = "USERS.country"
    sql_expression            = "USERS.COUNTRY"
  }

  facts {
    qualified_expression_name = "INVENTORY_ITEMS.product_retail_price"
    sql_expression            = "INVENTORY_ITEMS.PRODUCT_RETAIL_PRICE"
  }

  facts {
    qualified_expression_name = "PRODUCTS.retail_price"
    sql_expression            = "PRODUCTS.RETAIL_PRICE"
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ORDER_ITEMS.count"
      sql_expression            = "COUNT(DISTINCT ORDER_ITEMS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ORDER_ITEMS.sale_price_sum"
      sql_expression            = "SUM(ORDER_ITEMS.SALE_PRICE)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ORDER_ITEMS.avg_sale_price"
      sql_expression            = "AVG(ORDER_ITEMS.SALE_PRICE)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "USERS.count"
      sql_expression            = "COUNT(DISTINCT USERS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "USERS.age_average"
      sql_expression            = "AVG(USERS.AGE)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "INVENTORY_ITEMS.count"
      sql_expression            = "COUNT(DISTINCT INVENTORY_ITEMS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "PRODUCTS.count"
      sql_expression            = "COUNT(DISTINCT PRODUCTS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "DISTRIBUTION_CENTERS.count"
      sql_expression            = "COUNT(DISTINCT DISTRIBUTION_CENTERS.ID)"
    }
  }

}
