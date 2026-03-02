from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, count, sum as fsum, avg as favg,
    log1p, when, lit
)

BRONZE_PATH = "/opt/data/bronze_claims"
GOLD_PATH = "/opt/data/gold_features"
CHECKPOINT = "/opt/data/_chk_gold"

spark = SparkSession.builder.appName("claims-gold-features").getOrCreate()

bronze = spark.readStream.format("parquet").load(BRONZE_PATH)

df = (
    bronze
    .withColumn("event_ts", to_timestamp(col("submit_ts")))
    .withWatermark("event_ts", "5 minutes")
    .withColumn("log_claim_amount", log1p(col("claim_amount")))
    .withColumn("is_api", when(col("channel") == "api", 1).otherwise(0))
    .withColumn("no_attachments", when(col("attachments_count") == 0, 1).otherwise(0))
)

# Rolling 30 minute aggregates (demo). In prod you'd likely do 30d/90d via state stores or batch jobs.
member_agg = (
    df.groupBy(col("member_id"), window(col("event_ts"), "30 minutes", "5 minutes"))
    .agg(
        count(lit(1)).alias("member_claim_count_30m"),
        fsum(col("claim_amount")).alias("member_claim_amount_sum_30m"),
        favg(col("claim_amount")).alias("member_claim_amount_avg_30m"),
    )
)

provider_agg = (
    df.groupBy(col("provider_id"), window(col("event_ts"), "30 minutes", "5 minutes"))
    .agg(
        count(lit(1)).alias("provider_claim_count_30m"),
        favg(col("claim_amount")).alias("provider_claim_amount_avg_30m"),
    )
)

# Join back (align on window)
gold = (
    df.join(member_agg, on=["member_id", "window"], how="left")
    .join(provider_agg, on=["provider_id", "window"], how="left")
    .drop("window")
    .fillna(0, subset=[
        "member_claim_count_30m",
        "member_claim_amount_sum_30m",
        "member_claim_amount_avg_30m",
        "provider_claim_count_30m",
        "provider_claim_amount_avg_30m",
    ])
)

# Keep training columns + label
gold_out = gold.select(
    "claim_id", "member_id", "provider_id", "policy_id",
    "event_ts",
    "claim_amount", "log_claim_amount",
    "is_api", "no_attachments",
    "member_claim_count_30m", "member_claim_amount_sum_30m", "member_claim_amount_avg_30m",
    "provider_claim_count_30m", "provider_claim_amount_avg_30m",
    col("label_should_decline").cast("int").alias("label")
)

q = (
    gold_out.writeStream
    .format("parquet")
    .option("path", GOLD_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .start()
)

q.awaitTermination()