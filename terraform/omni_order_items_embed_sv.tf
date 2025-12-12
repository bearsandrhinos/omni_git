resource "snowflake_semantic_view" "omni_order_items_embed_sv" {
  database = var.snowflake_database
  schema   = "ECOMM"
  name     = "omni_order_items_embed_sv"
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

  tables {
    table_alias = "ECOMM_PRODUCTS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"PRODUCTS\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "DEMO_PRODUCT_IMAGES"
    table_name  = "${var.snowflake_database}.\"DEMO\".\"PRODUCT_IMAGES\""
    primary_key = ["ID"]
  }

  tables {
    table_alias = "ECOMM_DISTRIBUTION_CENTERS"
    table_name  = "${var.snowflake_database}.\"ECOMM\".\"DISTRIBUTION_CENTERS\""
    primary_key = ["ID"]
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.id"
    sql_expression            = "ECOMM_ORDER_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.status_ordering"
    sql_expression            = "CASE WHEN ECOMM_ORDER_ITEMS.STATUS = 'Complete' THEN 1 WHEN ECOMM_ORDER_ITEMS.STATUS = 'Shipped' THEN 2 WHEN ECOMM_ORDER_ITEMS.STATUS = 'Processing' THEN 3 WHEN ECOMM_ORDER_ITEMS.STATUS = 'Cancelled' THEN 4 WHEN ECOMM_ORDER_ITEMS.STATUS = 'Returned' THEN 5 ELSE 6 END"
    comment                   = "This actually will order the fields, trust me"
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
    qualified_expression_name = "ECOMM_ORDER_ITEMS.returned_at"
    sql_expression            = "ECOMM_ORDER_ITEMS.RETURNED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.shipped_at"
    sql_expression            = "ECOMM_ORDER_ITEMS.SHIPPED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.delivered_at"
    sql_expression            = "ECOMM_ORDER_ITEMS.DELIVERED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.time_to_ship"
    sql_expression            = "datediff('days', ECOMM_ORDER_ITEMS.CREATED_AT, ECOMM_ORDER_ITEMS.SHIPPED_AT)"
    comment                   = "Time between order created and order shipped"
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

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.id"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_id"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.created_at"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.CREATED_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.sold_at"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.SOLD_AT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.cost"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.COST"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_category"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_CATEGORY"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_name"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_NAME"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_brand"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_BRAND"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_department"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_DEPARTMENT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_sku"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_SKU"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_distribution_center_id"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_DISTRIBUTION_CENTER_ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.days_in_inventory"
    sql_expression            = "case when ECOMM_INVENTORY_ITEMS.SOLD_AT is null then datediff(day, ECOMM_INVENTORY_ITEMS.CREATED_AT, CURRENT_DATE()) else datediff(day, ECOMM_INVENTORY_ITEMS.CREATED_AT, ECOMM_INVENTORY_ITEMS.SOLD_AT) end"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.id"
    sql_expression            = "ECOMM_PRODUCTS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.cost"
    sql_expression            = "ECOMM_PRODUCTS.COST"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.category"
    sql_expression            = "ECOMM_PRODUCTS.CATEGORY"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.name_embed"
    sql_expression            = "replace(\"ECOMM_PRODUCTS\".\"NAME\", ECOMM_PRODUCTS.BRAND)"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.name"
    sql_expression            = "ECOMM_PRODUCTS.NAME"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.brand"
    sql_expression            = "ECOMM_PRODUCTS.BRAND"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.department"
    sql_expression            = "ECOMM_PRODUCTS.DEPARTMENT"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.sku"
    sql_expression            = "ECOMM_PRODUCTS.SKU"
  }

  dimensions {
    qualified_expression_name = "ECOMM_PRODUCTS.distribution_center_id"
    sql_expression            = "ECOMM_PRODUCTS.DISTRIBUTION_CENTER_ID"
  }

  dimensions {
    qualified_expression_name = "DEMO_PRODUCT_IMAGES.id"
    sql_expression            = "DEMO_PRODUCT_IMAGES.ID"
  }

  dimensions {
    qualified_expression_name = "DEMO_PRODUCT_IMAGES.image"
    sql_expression            = "DEMO_PRODUCT_IMAGES.IMAGE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_DISTRIBUTION_CENTERS.id"
    sql_expression            = "ECOMM_DISTRIBUTION_CENTERS.ID"
  }

  dimensions {
    qualified_expression_name = "ECOMM_DISTRIBUTION_CENTERS.name"
    sql_expression            = "ECOMM_DISTRIBUTION_CENTERS.NAME"
  }

  dimensions {
    qualified_expression_name = "ECOMM_DISTRIBUTION_CENTERS.latitude"
    sql_expression            = "ECOMM_DISTRIBUTION_CENTERS.LATITUDE"
  }

  dimensions {
    qualified_expression_name = "ECOMM_DISTRIBUTION_CENTERS.longitude"
    sql_expression            = "ECOMM_DISTRIBUTION_CENTERS.LONGITUDE"
  }

  facts {
    qualified_expression_name = "ECOMM_ORDER_ITEMS.sale_price"
    sql_expression            = "ECOMM_ORDER_ITEMS.SALE_PRICE"
  }

  facts {
    qualified_expression_name = "ECOMM_USERS.country"
    sql_expression            = "ECOMM_USERS.COUNTRY"
  }

  facts {
    qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_retail_price"
    sql_expression            = "ECOMM_INVENTORY_ITEMS.PRODUCT_RETAIL_PRICE"
  }

  facts {
    qualified_expression_name = "ECOMM_PRODUCTS.retail_price"
    sql_expression            = "ECOMM_PRODUCTS.RETAIL_PRICE"
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
      sql_expression            = "SUM(ECOMM_ORDER_ITEMS.SALE_PRICE * 0.90)"
      comment                   = "total revenue from orders and order items"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_ORDER_ITEMS.time_to_ship_average"
      sql_expression            = "AVG(ECOMM_ORDER_ITEMS.TIME_TO_SHIP)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_ORDER_ITEMS.total_revenue"
      sql_expression            = "SUM(CASE WHEN ECOMM_ORDER_ITEMS.STATUS = 'Complete' THEN ECOMM_ORDER_ITEMS.SALE_PRICE ELSE NULL END)"
      comment                   = "this is complted orders"
    }
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

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.count"
      sql_expression            = "COUNT(DISTINCT ECOMM_INVENTORY_ITEMS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.inventory_volume"
      sql_expression            = "COUNT(DISTINCT ECOMM_INVENTORY_ITEMS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.cost_sum"
      sql_expression            = "SUM(ECOMM_INVENTORY_ITEMS.COST)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.days_in_inventory_average"
      sql_expression            = "AVG(ECOMM_INVENTORY_ITEMS.DAYS_IN_INVENTORY)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.product_retail_price_average"
      sql_expression            = "AVG(ECOMM_INVENTORY_ITEMS.PRODUCT_RETAIL_PRICE)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_INVENTORY_ITEMS.test_measure"
      sql_expression            = "SUM((uniform(1, 2.01::number(10,2), RANDOM())))"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_PRODUCTS.count"
      sql_expression            = "COUNT(DISTINCT ECOMM_PRODUCTS.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "DEMO_PRODUCT_IMAGES.count"
      sql_expression            = "COUNT(DISTINCT DEMO_PRODUCT_IMAGES.ID)"
    }
  }

  metrics {
    semantic_expression {
      qualified_expression_name = "ECOMM_DISTRIBUTION_CENTERS.count"
      sql_expression            = "COUNT(DISTINCT ECOMM_DISTRIBUTION_CENTERS.ID)"
    }
  }

}
