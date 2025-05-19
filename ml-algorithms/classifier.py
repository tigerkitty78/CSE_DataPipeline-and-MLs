from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lag, regexp_replace, to_date, avg, stddev, when, last, max as sp_max, count
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Initialize Spark Session
spark = SparkSession.builder.appName("StockPriceClassification").getOrCreate()

# Load Data
df = spark.read.csv("combined_daily_data.csv", header=True, inferSchema=True)

# Convert 'Date' column to proper date format
df = df.withColumn("date", to_date(regexp_replace(col("Date"), ".csv", ""), "yyyyMMdd"))

# Define window specifications
window_lag = Window.partitionBy("name").orderBy("date")
window_agg = Window.partitionBy("name").orderBy("date").rowsBetween(-3, -1)

# Feature Engineering
df = (df
      .withColumn("Next_Price", lag("price", -1).over(window_lag))
      .withColumn("Label", when(col("Next_Price") > col("price"), 1).otherwise(0))
      .withColumn("MA_3", avg("price").over(window_agg))
      .withColumn("Volatility_3", stddev("price").over(window_agg)))

# Add more lag features and momentum indicators
lags = [1, 2, 3, 5, 7]
for lag_days in lags:
    df = df.withColumn(f"PRICE_LAG_{lag_days}", lag("price", lag_days).over(window_lag))

df = (df
      .withColumn("3DAY_MOMENTUM", col("price") - lag("price", 3).over(window_lag))
      .withColumn("7DAY_MA", avg("price").over(window_agg.rowsBetween(-7, -1)))
      .withColumn("PRICE_CHANGE_3D", (col("price") - lag("price", 3).over(window_lag)) / lag("price", 3).over(window_lag)))

# Forward fill nulls for moving average and volatility
window_ffill = Window.partitionBy("name").orderBy("date")
for col_name in ["MA_3", "Volatility_3"]:
    df = df.withColumn(col_name, last(col_name, ignorenulls=True).over(window_ffill))

# Remove rows where 'Label' is null
df = df.dropna(subset=["Label"])
feature_cols = ["MA_3", "Volatility_3", "3DAY_MOMENTUM", "7DAY_MA", "PRICE_CHANGE_3D"] + [f"PRICE_LAG_{lag}" for lag in lags]
for col_name in feature_cols:
    df = df.withColumn(col_name, col(col_name).cast("double"))

df = df.dropna(subset=feature_cols)

# Get the last available date in the dataset
max_date = df.select(sp_max("date")).collect()[0][0]

# Select last 7 days for testing, all previous data for training
train_df = df.filter(col("date") < (max_date - expr("INTERVAL 7 DAYS")))
test_df = df.filter(col("date") >= (max_date - expr("INTERVAL 7 DAYS")))

# Check class distribution
train_df.groupBy("Label").count().show()

# Handle class imbalance
class_counts = train_df.groupBy("Label").count().rdd.collectAsMap()
total = sum(class_counts.values())
weight_0 = total / (2.0 * class_counts.get(0, 1))
weight_1 = total / (2.0 * class_counts.get(1, 1))

train_df = train_df.withColumn("class_weight", when(col("Label") == 0, weight_0).otherwise(weight_1))

# Assemble Features
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_df = assembler.transform(train_df).select("features", col("Label").cast("double"), "class_weight")
test_df = assembler.transform(test_df).select("features", col("Label").cast("double"))

# Train Gradient Boosted Tree (GBT) Model
gbt = GBTClassifier(featuresCol="features", labelCol="Label", maxIter=50, maxDepth=7, stepSize=0.1, subsamplingRate=0.8, maxBins=50, weightCol="class_weight")

model = gbt.fit(train_df)

# Generate Predictions
predictions = model.transform(test_df)

# Evaluation Metrics
evaluator = BinaryClassificationEvaluator(labelCol="Label")
print(f"AUC-ROC: {evaluator.evaluate(predictions)}")
predictions.groupBy("Label", "prediction").count().show()

correct_preds = predictions.filter(col("Label") == col("prediction")).count()
total_preds = predictions.count()
accuracy = (correct_preds / total_preds) * 100 if total_preds > 0 else 0
print(f"Accuracy: {accuracy:.2f}%")

# Print first 20 rows of actual vs predicted labels
predictions.select("Label", "prediction").show(20)

# Feature Importance
feature_importance = model.featureImportances
sorted_features = sorted(zip(feature_importance.indices, feature_importance.values), key=lambda x: -x[1])
print("Feature Importance:")
for idx, importance in sorted_features:
    print(f"{feature_cols[idx]}: {importance:.4f}")

# Stop Spark Session
spark.stop()
