# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
BASE = "/Volumes/learning/bronze/raw"

orders_raw = (
    spark.read
        .option("header", True)
       # .option("inferSchema", True)
        .csv(f"{BASE}/orders")
)

orders_raw.printSchema()
display(orders_raw.limit(10))


# COMMAND ----------

orders_schema = """
    order_id    INT,
    customer_id INT,
    product_id  INT,
    quantity    INT,
    unit_price  DOUBLE,
    order_timestamp TIMESTAMP,
    status      STRING
"""

orders = (
    spark.read
        .schema(orders_schema)
        .option("header", True)
        .csv(f"{BASE}/orders")
)

orders.printSchema()
display(orders)
#

# COMMAND ----------

big_orders = orders.filter(orders.quantity >= 4)
#print("Defined big_orders. Nothing has been computed yet!")
display(big_orders.limit(10000))
print("Displayed big_orders. It has been computed now!")


# COMMAND ----------

from pyspark.sql import functions as F

orders_enriched = (
    orders
        .select("order_id", "customer_id", "product_id", "quantity", "unit_price", "status")
        .filter(F.col("quantity") > 0)
        .withColumn("line_total",
            F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("Status_clean",
            F.lower(F.trim(F.col("status"))))
)
display(orders_enriched)

# COMMAND ----------

print(big_orders.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, quantity*unit_price AS line_total, lower(trim(status)) AS status_clean
# MAGIC FROM read_files('/Volumes/learning/bronze/raw/orders/', format => 'csv', header => true)
# MAGIC WHERE quantity > 0
# MAGIC LIMIT 10;

# COMMAND ----------

display(
    orders.groupBy("quantity").count().orderBy("quantity")
)

# COMMAND ----------

orders_schema_with_corrupt = orders_schema + ", _corrupt_record STRING"

orders_checked = (
    spark.read
        .schema(orders_schema_with_corrupt)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(f"{BASE}/orders")
)
display(orders_checked)

# COMMAND ----------

customers = (
    spark.read.option("header", True)
    .schema("customer_id INT, name STRING, country STRING, signedup_date DATE")
        .csv(f"{BASE}/customers")
)

products = (
    spark.read.option("header", "true")
    .schema("prouct_id INT, product_name STRING, category STRING, price DOUBLE ")
    .csv(f"{BASE}/products/")
)

print("customers: ", customers.count())
print("products: ", products.count())
display(customers)
display(products)

# COMMAND ----------

print("all rows:                   ", orders.count())
print("full-row duplicates removed:", orders.dropDuplicates().count())
print("unique order_ids:           ", orders.dropDuplicates(["order_id"]).count())

# COMMAND ----------

customer_dedup = customers.dropDuplicates(["customer_id"])
print("customers: ", customers.count())
print("deduped customers: ", customer_dedup.count())

# COMMAND ----------

with_dup    = orders_enriched.join(customers,   "customer_id", "inner").count()
without_dup = orders_enriched.join(customer_dedup, "customer_id", "inner").count()

print("join with duplicate customer:", with_dup)
print("join after dedup:           ", without_dup)

# COMMAND ----------

display(orders.filter("customer_id = 6"))


# COMMAND ----------

customer_doubled = customer_dedup.union(customer_dedup)
display(customer_doubled)
display(customer_doubled.filter("customer_id = 12"))
