from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "claims_raw")

BRONZE_PATH = "/opt/data/bronze_claims"
CHECKPOINT = "/opt/data/_chk_bronze"

schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("member_id", StringType(), False),
    StructField("policy_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("claim_amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("attachments_count", IntegerType(), True),
    StructField("submit_ts", StringType(), False),
    StructField("label_should_decline", IntegerType(), True),
])

spark = (
    SparkSession.builder
    .appName("claims-bronze-ingest")
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
)

q = (
    parsed.writeStream
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .start()
)

q.awaitTermination()