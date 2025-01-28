from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, max as spark_max
import os

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("IncrementalLoadToDelta") \
    .config("spark.jars", "/path/to/postgresql.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
jdbc_properties = {
    "user": "mt24042",
    "password": "mt24042@m06y24",
    "driver": "org.postgresql.Driver"
}
source_table = "employee_projects_mt24042"
delta_table_path = "modak-training-bucket1/mt24042/jdbcinc/"
increment_column = "id"  # E.g., timestamp or primary key column

# File to store the last processed watermark
watermark_file = "/path/to/watermark.txt"

# Step 1: Get the last processed watermark
def get_last_watermark():
    if os.path.exists(watermark_file):
        with open(watermark_file, "r") as file:
            return file.read().strip()
    return None

last_watermark = get_last_watermark()

# Step 2: Load data incrementally from the JDBC source
query = f"(SELECT * FROM {source_table} WHERE {increment_column} > '{last_watermark}') AS incremental_data" if last_watermark else f"(SELECT * FROM {source_table}) AS full_data"
jdbc_df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)

# Step 3: Perform data mappings or transformations (example mappings shown)
transformed_df = jdbc_df.withColumnRenamed("old_column1", "new_column1") \
                        .withColumnRenamed("old_column2", "new_column2") \
                        .select("new_column1", "new_column2", "other_required_columns")

# Step 4: Merge the data into the Delta table
if DeltaTable.isDeltaTable(spark, delta_table_path):
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.primary_key = source.primary_key"  # Replace with appropriate key column(s)
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    # Create Delta table if not exists
    transformed_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Step 5: Update the watermark
if not jdbc_df.rdd.isEmpty():
    new_watermark = jdbc_df.agg(spark_max(col(increment_column))).collect()[0][0]
    with open(watermark_file, "w") as file:
        file.write(str(new_watermark))

print("Incremental load completed successfully.")

# Stop Spark session
spark.stop()

