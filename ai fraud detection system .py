#!/usr/bin/env python3
"""
AI-based Fraud Detection System: HDFS + PySpark + classification models.

Dataset : Kaggle "Credit Card Fraud Detection" (mlg-ulb/creditcardfraud), file creditcard.csv
          (columns: Time, V1..V28 [PCA-anonymised], Amount, Class). Download needs a free Kaggle login.
Needs   : Java 8/11/17, Hadoop 3.x (HDFS running, `hdfs` on PATH), pip install pyspark
Run     : spark-submit fraud_detection.py --csv creditcard.csv --hdfs-dir /user/$USER/fraud

Ethics built in:
  1. Refuses to run if the file has columns that look like personal data.
  2. Uses only anonymised features; logs only aggregate numbers, never row data.
  3. Handles class imbalance with weights instead of discarding data.
  4. Flagged transactions go to a human review queue; nothing is blocked automatically.

Suryansh, B.Tech data science student
"""
import argparse
import csv
import json
import logging
import os
import shutil
import subprocess
import sys

try:
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import DoubleType, StructField, StructType
except ImportError as exc:
    raise SystemExit(
        "PySpark is not installed. Install Java 8/11/17 and run: pip install pyspark"
    ) from exc
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

FEATURES = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
LABEL = "Class"
PII_HINTS = ("name", "email", "phone", "address", "ssn", "aadhaar", "dob", "cardnumber", "account", "ip")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fraud")


def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {result.stderr.strip()}")


def ethics_check(csv_path):
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        header = [c.strip() for c in next(csv.reader(f), [])]
    expected = set(FEATURES + [LABEL])
    unexpected = [c for c in header if c not in expected]
    risky = [c for c in unexpected if any(h in c.lower() for h in PII_HINTS)]
    if risky:
        raise ValueError(f"Possible personal data columns found: {risky}. Remove or anonymise them first.")
    if set(header) != expected:
        raise ValueError(f"Unexpected schema. Missing: {sorted(expected - set(header))}, extra: {unexpected}")


def upload_to_hdfs(local_path, hdfs_dir):
    if not shutil.which("hdfs"):
        local_dir = os.path.abspath(hdfs_dir)
        os.makedirs(local_dir, exist_ok=True)
        target = os.path.join(local_dir, os.path.basename(local_path))
        shutil.copy2(local_path, target)
        log.warning("HDFS command not found; using local filesystem instead: %s", target)
        return target.replace("\\", "/")
    run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_dir])
    return f"{hdfs_dir.rstrip('/')}/{os.path.basename(local_path)}"


def load_data(spark, hdfs_path):
    schema = StructType([StructField(c, DoubleType(), True) for c in FEATURES + [LABEL]])
    df = spark.read.csv(hdfs_path, header=True, schema=schema, mode="FAILFAST")
    df = df.dropna().dropDuplicates().filter(F.col(LABEL).isin(0.0, 1.0))
    if df.rdd.isEmpty():
        raise ValueError("No valid rows after cleaning.")
    return df


def stratified_split(df, train_ratio=0.8, seed=42):
    parts = [df.filter(F.col(LABEL) == c).randomSplit([train_ratio, 1 - train_ratio], seed) for c in (0.0, 1.0)]
    return parts[0][0].union(parts[1][0]), parts[0][1].union(parts[1][1])


def add_class_weights(train):
    counts = {r[LABEL]: r["count"] for r in train.groupBy(LABEL).count().collect()}
    if set(counts) != {0.0, 1.0}:
        raise ValueError("Training data must contain both normal and fraud transactions.")
    total = sum(counts.values())
    w0, w1 = total / (2 * counts[0.0]), total / (2 * counts[1.0])
    return train.withColumn("weight", F.when(F.col(LABEL) == 1.0, w1).otherwise(w0))


def build_pipeline(classifier):
    return Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
        classifier,
    ])


def evaluate(pred):
    roc = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderROC").evaluate(pred)
    pr = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderPR").evaluate(pred)
    c = {(r[LABEL], r["prediction"]): r["count"] for r in pred.groupBy(LABEL, "prediction").count().collect()}
    tp, fp = c.get((1.0, 1.0), 0), c.get((0.0, 1.0), 0)
    fn, tn = c.get((1.0, 0.0), 0), c.get((0.0, 0.0), 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"auc_roc": roc, "auc_pr": pr, "precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn}


def main():
    parser = argparse.ArgumentParser(description="Fraud detection with HDFS and PySpark", allow_abbrev=False)
    parser.add_argument(
        "--csv",
        default="creditcard.csv",
        help="Path to local creditcard.csv (default: creditcard.csv in the current directory)",
    )
    parser.add_argument(
        "--hdfs-dir",
        default="fraud_data",
        help="HDFS working directory, or local fallback directory (default: fraud_data)",
    )
    parser.add_argument("--metrics-out", default="fraud_metrics.json")
    args = parser.parse_args()

    if not os.path.isfile(args.csv):
        log.error("File not found: %s", args.csv)
        return 1

    spark = None
    try:
        ethics_check(args.csv)
        hdfs_path = upload_to_hdfs(args.csv, args.hdfs_dir)
        log.info("Data stored in HDFS: %s", hdfs_path)

        spark = SparkSession.builder.appName("fraud-detection").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        df = load_data(spark, hdfs_path)
        train, test = stratified_split(df)
        train = add_class_weights(train).cache()
        test = test.cache()
        log.info("Train rows: %d | Test rows: %d", train.count(), test.count())

        candidates = {
            "logistic_regression": LogisticRegression(featuresCol="features", labelCol=LABEL,
                                                      weightCol="weight", maxIter=100, regParam=0.01),
            "random_forest": RandomForestClassifier(featuresCol="features", labelCol=LABEL,
                                                    weightCol="weight", numTrees=100, maxDepth=8, seed=42),
        }

        results, fitted, predictions = {}, {}, {}
        for name, clf in candidates.items():
            log.info("Training %s", name)
            model = build_pipeline(clf).fit(train)
            pred = model.transform(test).cache()
            fitted[name], predictions[name] = model, pred
            results[name] = evaluate(pred)
            log.info("%s -> %s", name, {k: round(v, 4) if isinstance(v, float) else v
                                        for k, v in results[name].items()})

        best = max(results, key=lambda n: results[n]["auc_pr"])
        log.info("Best model by AUC-PR: %s", best)

        fitted[best].write().overwrite().save(f"{args.hdfs_dir.rstrip('/')}/models/{best}")

        review = (predictions[best].filter(F.col("prediction") == 1.0)
                  .select("Time", "Amount", vector_to_array("probability")[1].alias("fraud_probability"))
                  .orderBy(F.desc("fraud_probability")))
        review.write.mode("overwrite").parquet(f"{args.hdfs_dir.rstrip('/')}/review_queue")

        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump({"best_model": best, "results": results}, f, indent=2)
        log.info("Metrics saved to %s", args.metrics_out)

        print("Suryansh, B.Tech data science student")
        return 0
    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())