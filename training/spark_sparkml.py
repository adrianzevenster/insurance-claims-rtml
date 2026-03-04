import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

GOLD_PATH = "/opt/data/gold_features"
MODEL_NAME = "claims-approval-model"

spark = SparkSession.builder.appName("train-claims-sparkml").getOrCreate()

df = spark.read.parquet(GOLD_PATH).dropna(subset=["label"])

feature_cols = [
    "log_claim_amount",
    "is_api",
    "no_attachments",
    "member_claim_count_30m",
    "member_claim_amount_sum_30m",
    "member_claim_amount_avg_30m",
    "provider_claim_count_30m",
    "provider_claim_amount_avg_30m",
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
clf = GBTClassifier(featuresCol="features", labelCol="label", maxDepth=5, maxIter=50)

pipeline = Pipeline(stages=[assembler, clf])

train, test = df.randomSplit([0.8, 0.2], seed=42)

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
mlflow.set_experiment("insurance-claims-approval")

with mlflow.start_run():
    model = pipeline.fit(train)
    preds = model.transform(test)

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(preds)

    mlflow.log_param("model", "GBTClassifier")
    mlflow.log_param("maxDepth", 5)
    mlflow.log_param("maxIter", 50)
    mlflow.log_metric("auc", float(auc))

    mlflow.spark.log_model(model, artifact_path="spark-model", registered_model_name=MODEL_NAME)
    print("AUC:", auc)

spark.stop()