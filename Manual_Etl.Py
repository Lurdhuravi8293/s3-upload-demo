import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Replace with your actual S3 paths
s3_input_path_customer = "s3://my-raw-data-bucket-12345/customer.csv"
s3_input_path_account = "s3://my-raw-data-bucket-12345/account.csv"
s3_input_path_transaction = "s3://my-raw-data-bucket-12345/transaction.csv"
s3_output_path_summary = "s3://my-staged-data-bucket-12345/customer_transaction_summary/"

# 1. Load CSV files from S3
df_customer = spark.read.option("header", "true").csv(s3_input_path_customer)
df_account = spark.read.option("header", "true").csv(s3_input_path_account)
df_transaction = spark.read.option("header", "true").csv(s3_input_path_transaction)

# 2. Convert amount to float
df_transaction = df_transaction.withColumn("amount", col("amount").cast("float"))

# 3. Filter transactions greater than $1000
df_transaction_filtered = df_transaction.filter(col("amount") > 1000)

# 4. Add transaction fee column (2% of amount)
df_transaction_filtered = df_transaction_filtered.withColumn("fee", col("amount") * 0.02)

# 5. Join customer with account on customer_id
df_cust_acc = df_customer.join(df_account, on="customer_id", how="inner")

# 6. Join with transaction on account_id
df_full = df_cust_acc.join(df_transaction_filtered, on="account_id", how="inner")

# 7. Group by customer and sum amount
df_summary = df_full.groupBy("customer_id", "first_name", "last_name") \
                    .sum("amount") \
                    .withColumnRenamed("sum(amount)", "total_spent")

# 8. Save result to S3 as CSV
df_summary.write.mode("overwrite").option("header", "true").csv(s3_output_path_summary)
