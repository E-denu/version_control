from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("age").getOrCreate()

# Load CSV from HDFS
input_path = "hdfs://localhost:9000/emma/league/input/clinrad1.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Compute total goals for each team
df.createOrReplaceTempView("clinical_data")

age_group = spark.sql("SELECT * FROM clinical_data  WHERE age > 20 AND Sex = 'Male'")

# Output path in HDFS
output_path = "hdfs://localhost:9000/emma/league/output/"

# Save results to HDFS
age_group.write.mode("overwrite").csv(output_path)

# Stop SparkSession
spark.stop()
