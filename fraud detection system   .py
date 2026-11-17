#!/usr/bin/env python3
"""
AI-based Fraud Detection System: HDFS + PySpark + classification models.

Dataset : Kaggle "Credit Card Fraud Detection" (mlg-ulb/creditcardfraud), file creditcard.csv
          (columns: Time, V1..V28 [PCA-anonymised], Amount, Class). Download needs a free Kaggle login.
Needs   : Java 8/11/17, Hadoop 3.x (HDFS running, `hdfs` on PATH), pip install pyspark
Run     : spark-submit fraud_detection.py
          (auto-finds creditcard.csv or creditcard.zip in the current or script folder)
          spark-submit fraud_detection.py --csv /full/path/creditcard.csv --hdfs-dir /user/<you>/fraud

Ethics built in:
  1. Refuses to run if the file has columns that look like personal data.
  2. Uses only anonymised features; logs only aggregate numbers, never row data.
  3. Handles class imbalance with weights instead of discarding data.
  4. Flagged transactions go to a human review queue; nothing is blocked automatically.

Suryansh, B.Tech data science student
"""
import argparse
import csv
import getpass
import glob
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile

from pyspark.ml import Pipeline
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


def nearby_files():
    folders = {os.getcwd(), os.path.dirname(os.path.abspath(__file__))}
    found = []
    for folder in sorted(folders):
        for pattern in ("creditcard*.csv", "creditcard*.zip"):
            found += sorted(glob.glob(os.path.join(folder, pattern)))
    return found


def extract_zip(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        members = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"No .csv file inside {zip_path}")
        target = os.path.join(os.path.dirname(zip_path), os.path.basename(members[0]))
        with z.open(members[0]) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst)
    log.info("Extracted %s", target)
    return target


def resolve_csv(arg):
    if arg:
        path = os.path.abspath(os.path.expanduser(arg))
        if not os.path.isfile(path):
            hint = nearby_files()
            raise FileNotFoundError(
                f"File not found: {path}. Check the name ends with .csv."
                + (f" Similar files found: {hint}" if hint else ""))
    else:
        files = nearby_files()
        if not files:
            raise FileNotFoundError(
                "creditcard.csv not found in the current or script folder. "
                "Download it from Kaggle (mlg-ulb/creditcardfraud) or pass --csv /full/path/creditcard.csv")
        path = files[0]
    if path.lower().endswith(".zip"):
        path = extract_zip(path)
    if not path.lower().endswith(".csv"):
        raise ValueError(f"Expected a .csv file, got: {path}")
    return path


def ethics_check(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f), [])
    expected = set(FEATURES + [LABEL])
    unexpected = [c for c in header if c not in expected]
    risky = [c for c in unexpected if any(h in c.lower() for h in PII_HINTS)]
    if risky:
        raise ValueError(f"Possible personal data columns found: {risky}. Remove or anonymise them first.")
    if set(header) != expected:
        raise ValueError(f"Unexpected schema. Missing: {sorted(expected - set(header))}, extra: {unexpected}")


def upload_to_hdfs(local_path, hdfs_dir):
    if not shutil.which("hdfs"):
        raise RuntimeError("`hdfs` command not found. Install Hadoop and add its bin folder to PATH.")
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
    parser = argparse.ArgumentParser(description="Fraud detection with HDFS and PySpark")
    parser.add_argument("--csv", default=None, help="Path to creditcard.csv (auto-detected if omitted)")
    parser.add_argument("--hdfs-dir", default=f"/user/{getpass.getuser()}/fraud", help="HDFS working directory")
    parser.add_argument("--metrics-out", default="fraud_metrics.json")
    args = parser.parse_args()

    spark = None
    try:
        csv_path = resolve_csv(args.csv)
        log.info("Using file: %s", csv_path)
        ethics_check(csv_path)
        hdfs_path = upload_to_hdfs(csv_path, args.hdfs_dir)
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

        base = args.hdfs_dir.rstrip("/")
        fitted[best].write().overwrite().save(f"{base}/models/{best}")

        review = (predictions[best].filter(F.col("prediction") == 1.0)
                  .select("Time", "Amount", vector_to_array("probability")[1].alias("fraud_probability"))
                  .orderBy(F.desc("fraud_probability")))
        review.write.mode("overwrite").parquet(f"{base}/review_queue")

        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump({"best_model": best, "results": results}, f, indent=2)
        log.info("Metrics saved to %s", args.metrics_out)

        print("Suryansh, B.Tech data science student")
        return 0
    except Exception as exc:
        log.error("Pipeline failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())