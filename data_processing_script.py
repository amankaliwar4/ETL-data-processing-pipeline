from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, avg, to_date, current_timestamp
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL_Data_Processing").getOrCreate()

# Define Storage Paths
adls_raw_path = "abfss://container@storageaccount.dfs.core.windows.net/raw_data/transactions.csv"
adls_bronze_path = "abfss://container@storageaccount.dfs.core.windows.net/bronze/"
adls_silver_path = "abfss://container@storageaccount.dfs.core.windows.net/silver/"
adls_gold_path = "abfss://container@storageaccount.dfs.core.windows.net/gold/"

# Fetch Secrets from Azure Key Vault
key_vault_url = "https://your-keyvault-name.vault.azure.net"
credential = ManagedIdentityCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)
db_user = client.get_secret("synapse-db-user").value
db_password = client.get_secret("synapse-db-password").value
synapse_url = client.get_secret("synapse-db-url").value

# Read Raw Data from ADLS (Bronze Layer)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(adls_raw_path)

# Write Raw Data to Bronze Layer
df.write.mode("overwrite").parquet(adls_bronze_path)

# Data Cleaning & Transformation (Silver Layer Processing)
df_cleaned = df.fillna({"customer_id": "Unknown", "transaction_amount": 0, "transaction_date": "1970-01-01"})
df_cleaned = df_cleaned.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
df_transformed = df_cleaned.withColumn(
    "transaction_category",
    when(col("transaction_amount") >= 12000, "High Value")
    .when(col("transaction_amount") >= 5000, "Medium Value")
    .otherwise("Low Value")
).withColumn("processed_timestamp", current_timestamp())

# Write Processed Data to Silver Layer
df_transformed.write.mode("overwrite").parquet(adls_silver_path)

# Aggregation & Business Logic (Gold Layer Processing)
df_aggregated = df_transformed.groupBy("customer_id").agg(
    count("transaction_id").alias("total_transactions"),
    sum("transaction_amount").alias("total_spent"),
    avg("transaction_amount").alias("avg_transaction_value")
)

# Write Final Processed Data to Gold Layer
df_aggregated.write.mode("overwrite").format("delta").save(adls_gold_path)

# Load Data to Synapse SQL (For Analytics)
df_aggregated.write.format("jdbc").option("url", synapse_url)\
    .option("dbtable", "gold_transaction_summary")\
    .option("user", db_user)\
    .option("password", db_password)\
    .save()

