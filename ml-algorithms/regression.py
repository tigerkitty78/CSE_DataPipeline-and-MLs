from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, avg, stddev, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col, lag, lead, regexp_replace, to_date, avg, stddev, last,isnan
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import PCA
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.sql.types import DoubleType, FloatType
from pyspark.sql.functions import abs
from pyspark.ml.feature import RobustScaler
from pyspark.sql.functions import expr
from sklearn.metrics import confusion_matrix
from sklearn.metrics import mean_squared_error, r2_score,mean_absolute_error
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import numpy as np
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler
print(xgb.__version__) 
import pandas as pd
from sklearn.model_selection import RandomizedSearchCV
from sklearn.preprocessing import RobustScaler
import joblib
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StockPrediction") \
    .config("spark.sql.shuffle.partitions", "30") \
    .config("spark.executor.memory", "4g") \
    .config("spark.pyspark.python", r"C:\Python312\python.exe") \
    .config("spark.pyspark.driver.python", r"C:\Python312\python.exe") \
    .getOrCreate()

# Data Loading & Preprocessing ===============================================
# Load price data

# Load Data
price_df = spark.read.csv("combined_daily_data.csv", header=True, inferSchema=True)
finance_df = spark.read.csv("outputNew_cleaned2.csv",
                             header=True, inferSchema=True).withColumn("Total_Revenue", col("Total_Revenue").cast("double")) \
    .withColumn("Net_Income", col("Net_Income").cast("double")) \
    .withColumn("Basic_EPS", col("Basic_EPS").cast("double")) \
    .fillna({"Total_Revenue": 0, "Net_Income": 0, "Basic_EPS": 0}).withColumnRenamed("Symbol", "symbol")
# Parse dates
price_df = price_df.withColumn("date", to_date(regexp_replace(col("Date"), ".csv", ""), "yyyyMMdd"))

# Get column names and data types
column_dtypes = {col.name: col.dataType for col in price_df.schema}

# Generate aggregation expressions dynamically
aggregation_exprs = []
for column_name, dtype in column_dtypes.items():
    if isinstance(dtype, (DoubleType, FloatType)):
        # For numeric columns, check isnull OR isnan
        expr = count(when(isnull(col(column_name)) | isnan(col(column_name)), col(column_name))).alias(column_name)
    else:
        # For non-numeric (e.g., TIMESTAMP), check only isnull
        expr = count(when(isnull(col(column_name)), col(column_name))).alias(column_name)
    aggregation_exprs.append(expr)

# Apply aggregation
result = price_df.agg(*aggregation_exprs)

# Create Windows for Lag Features
window_lag = Window.partitionBy("symbol").orderBy("date")
window_5d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-5, -1)  # Changed to separate window
window_10d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-10, -1)
window_14d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-14, -1)
# Add Lag Features & Indicators
price_df = (price_df
    .withColumn("price_lag_1", lag("price", 1).over(window_lag))
    .withColumn("price_lag_2", lag("price", 2).over(window_lag))
    .withColumn("price_lag_3", lag("price", 3).over(window_lag))
    .withColumn("price_lag_5", lag("price", 5).over(window_lag))
    
    .withColumn("price_lag_7", lag("price", 7).over(window_lag))
   # Momentum and moving averages
    .withColumn("momentum_3", col("price") - lag("price", 3).over(window_lag))
    .withColumn("ma5", avg("price").over(window_5d))  # Using dedicated 5-day window
    .withColumn("ma10", avg("price").over(window_10d))
    .withColumn("mom5", col("price")/lag("price",5).over(window_lag) - 1)
    # Volatility and volume features (FIXED)
    .withColumn("volatility14", stddev("price").over(window_14d))
    .withColumn("volume_ma5", avg("tradevolume").over(window_5d))  # Correct window usage
    .withColumn("volume_spike", when(col("tradevolume") > 1.5 * col("volume_ma5"), 1).otherwise(0))
    .withColumn("vol_ma5", avg("tradevolume").over(window_5d))
    
    # RSI calculation (CORRECTED)
    .withColumn("avg_gain", 
               avg(when(col("price") > lag("price", 1).over(window_lag), 
                       col("price") - lag("price", 1).over(window_lag)).otherwise(0))
               .over(window_14d))
    .withColumn("avg_loss", 
               avg(when(col("price") < lag("price", 1).over(window_lag), 
                       lag("price", 1).over(window_lag) - col("price")).otherwise(0))
               .over(window_14d))
    # Add epsilon to avoid division by zero
    .withColumn("RSI", 100 - (100 / (1 + (col("avg_gain") / (col("avg_loss") + 1e-6)))))
    #####################################################################################
    # MACD calculation (CORRECTED)
    
     .withColumn("ema12", avg("price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-12, 0)))
    ##################################################################################3333                         
    .withColumn("ema26", avg("price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-26, 0)))
    .withColumn("MACD", col("ema12") - col("ema26"))
    # Target variable
    .withColumn("target", 
    when(
        isnull(lead("price", 1).over(window_lag)) | 
        isnan(lead("price", 1).over(window_lag)) | 
        isnull(col("price")) | 
        isnan(col("price")), 
        0.0
    ).otherwise(
         (lead("price", 1).over(window_lag) - col("price")) / col("price")   # ✅ True future price diff
    )
)
)
price_df.select("target").show(10)
price_df = price_df.join(finance_df, "symbol", "left")
 # NEW: Market data join

# NEW: Market-Relative Features
#price_df = price_df.withColumn("sp500_ma10", avg("sp500_close").over(Window.orderBy("date").rowsBetween(-10, 0)))
#price_df = price_df.withColumn("rel_to_market", col("price") / col("sp500_close"))

# Financial Ratios
price_df = price_df.withColumn("Book_Value", col("Total_Revenue") / col("sharevolume"))

price_df = price_df.withColumn("PE_Ratio", col("price") / col("Basic_EPS")).withColumn("PB_Ratio", col("price") / col("Book_Value")) \
.withColumn("Profit_Margin", col("Net_Income") / col("Total_Revenue"))

# NEW: Macro Features (example - needs actual data source)
price_df = price_df.withColumn("10y_yield", lit(0.0))  # Placeholder - load real data
price_df = price_df.withColumn("VIX", lit(0.0))  # Placeholder - load real data

######################################################################

# Convert PE_Ratio into a vector column
# vector_assembler = VectorAssembler(inputCols=["PE_Ratio"], outputCol="PE_Ratio_vec")
# price_df = vector_assembler.transform(price_df)  # Add PE_Ratio_vec

# # Apply RobustScaler
# scaler = RobustScaler(inputCol="PE_Ratio_vec", outputCol="scaled_PE")
# scaler_model = scaler.fit(price_df)
# price_df = scaler_model.transform(price_df)  # Add scaled_PE column

# # Check if scaled_PE was created
# price_df.select("PE_Ratio", "PE_Ratio_vec", "scaled_PE").show(5)
###############################################################feature normalization
# Feature Selection



# Create a label encoder
 # Ensure you use the same encoder for the test set

###########################################################



feature_cols = [
    "price_lag_1", "price_lag_2", "price_lag_3", "price_lag_5", "price_lag_7",
    "momentum_3", "ma5", "ma10", "volatility14",  # Added PB_Ratio
    "Profit_Margin", "RSI", "MACD", "volume_spike","mom5" ,"vol_ma5"# New technicals
    # Market features
]

# Handle Missing Data
price_df = price_df.fillna({col: 0 for col in feature_cols})

price_df = price_df.filter(
    (col("target") != 0.0) &  # Remove default zeros
    ~isnull(col("target")) &  # Remove nulls
    ~isnan(col("target"))     # Remove NaNs
)



#############################################
# Check nulls introduced by window operations
# Check null counts in key columns
from pyspark.sql.functions import col, sum, when

null_counts = price_df.select(*[
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)  # Added closing )
    for c in ["price_lag_7", "ma10", "RSI", "MACD"]
])

null_counts.show()

############################3

price_df.groupBy("target").count().orderBy("target").show(50)

# Fill feature nulls with 0 (adjust strategy as needed)
price_df = price_df.fillna(0, subset=["price_lag_7", "ma10", "RSI", "MACD"])

print("Total Rows:", price_df.count())
print("Feature Null Counts:")
null_counts.show()


# Train-Test Split (remains same)
##################################### Keep 30 days for testing (adjust based on your data)
split_date = price_df.selectExpr("date_sub(max(date), 14)").first()[0]
train_df = price_df.filter(col("date") < split_date)
test_df = price_df.filter(col("date") >= split_date)


print(f"Train count: {train_df.count()}, Test count: {test_df.count()}")
###################convert spark df to pandas
train_pandas = train_df.toPandas()
test_pandas = test_df.toPandas()



X_train = train_pandas.drop(columns=["target"])  
y_train = train_pandas["target"]
X_test = test_pandas.drop(columns=["target"])
y_test = test_pandas["target"]
##########################################################



categorical_columns = ['symbol', 'name', 'Quarter']
for col in categorical_columns:
    X_train[col] = X_train[col].fillna("missing").astype(str)
    X_test[col] = X_test[col].fillna("missing").astype(str)
# Collect allowed categories from training data
allowed_categories = {col: set(X_train[col]) for col in categorical_columns}

# Create a mask for test data where all categorical columns are valid
mask = pd.Series(True, index=X_test.index)
for col in categorical_columns:
    mask &= X_test[col].isin(allowed_categories[col])
joblib.dump(allowed_categories, 'allowed_categories3.pkl')
# Apply the mask once
X_test = X_test[mask]
y_test = y_test[mask]

# Now apply LabelEncoder to remaining data

##################################################################
# Convert 'Scrape_time' and 'issueDate' to datetime if they aren't already
# Convert to datetime and extract features
X_train['Scrape_time'] = pd.to_datetime(X_train['Scrape_time'])
X_test['Scrape_time'] = pd.to_datetime(X_test['Scrape_time'])

for df in [X_train, X_test]:
    df['Scrape_time_year'] = df['Scrape_time'].dt.year
    df['Scrape_time_month'] = df['Scrape_time'].dt.month
    df['Scrape_time_day'] = df['Scrape_time'].dt.day

label_encoders = {}
for col in categorical_columns:
    le = LabelEncoder()
    le.fit(X_train[col].astype(str))  # Explicitly convert to string
    X_train[col] = le.transform(X_train[col])
    X_test[col] = le.transform(X_test[col])
    label_encoders[col] = le


# Save the dictionary of encoders
joblib.dump(label_encoders, 'label_encoders4.pkl')  
###########################################
# Drop the original non-numeric columns
# After converting Spark DF to Pandas:
selected_features = [
    "price_lag_1", "price_lag_2", "price_lag_3", "price_lag_5", "price_lag_7",
    "momentum_3", "ma5", "ma10", "volatility14", 
    "Profit_Margin", "RSI", "MACD", "volume_spike", "mom5", "vol_ma5"
]

X_train = train_pandas[selected_features]
y_train = train_pandas["target"]

X_test = test_pandas[selected_features]
y_test = test_pandas["target"]
##########################################333
####normalize the data


# Select numerical columns for normalization
numerical_cols = [
    "price_lag_1", "price_lag_2", "price_lag_3", "price_lag_5", "price_lag_7",
    "momentum_3", "ma5", "ma10", "volatility14", 
    "Profit_Margin", "RSI", "MACD", "volume_spike","mom5","vol_ma5"
]

# Initialize StandardScaler
scaler =  RobustScaler()

# Fit and transform the training data
X_train[numerical_cols] = scaler.fit_transform(X_train[numerical_cols])

# Transform the test data using the same scaler (to prevent data leakage)
X_test[numerical_cols] = scaler.transform(X_test[numerical_cols])

# Initialize a separate StandardScaler for y
y_scaler =  RobustScaler()

# Fit and transform the training target (ensure it's reshaped to 2D)
# y_train = y_scaler.fit_transform(y_train.reshape(-1, 1))

# # Transform the test target using the same scaler
# y_test = y_scaler.transform(y_test.reshape(-1, 1))





########################################################

# Convert to DMatrix
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

##################################################### xg boost algorithm

params = {
    'objective': 'reg:squarederror',  # For regression tasks
    'booster': 'gbtree',              # Using tree-based models
    'eval_metric': 'rmse',            # RMSE as the evaluation metric
    'learning_rate': 0.1,             # Learning rate
    'max_depth': 6,                   # Maximum tree depth                                      # Number of boosting rounds
    'subsample': 0.8,                 # Fraction of samples for each tree
    'colsample_bytree': 0.8,          # Fraction of features for each tree
}

print(X_train.shape, y_train.shape)
print(X_test.shape, y_test.shape)
evals = [(dtrain, 'train'), (dtest, 'test')]



############################################weight balancer



class WeightedXGBRegressor(xgb.XGBRegressor):
    def fit(self, X, y, **fit_params):
        # Calculate sample weights based on target direction
        y_series = pd.Series(y)
        positive = (y_series > 0)
        negative = ~positive

        positive_count = positive.sum()
        negative_count = negative.sum()
        total = positive_count + negative_count

        # Calculate weights inversely proportional to class frequency
   # Instead of hard 50/50 split (total/2), use actual class ratios
        weight_positive = total / positive_count if positive_count > 0 else 1.0
        weight_negative = total / negative_count if negative_count > 0 else 1.0

        sample_weights = np.where(positive, weight_positive, weight_negative)
        
        # Fit model with calculated weights
        super().fit(X, y, sample_weight=sample_weights, **fit_params)
        return self






######################################


# Train the XGBoost model with early stopping
# Define the model
xgb_model = xgb.XGBRegressor(objective='reg:squarederror', random_state=42)

# Define the parameter grid
param_grid = {
    'learning_rate': [0.01, 0.05, 0.1, 0.15, 0.2],
    'max_depth': [3, 4, 5, 6, 7, 8],
    'n_estimators': [50, 100, 200, 300],
    'subsample': [0.6, 0.7, 0.8, 0.9, 1.0],
    'colsample_bytree': [0.6, 0.7, 0.8, 0.9, 1.0],
    'gamma': [0, 0.1, 0.2, 0.3, 0.4],
    'min_child_weight': [1, 3, 5, 7, 10],
     'reg_alpha': [0, 0.1],           # L1 regularization
    'reg_lambda': [0, 0.1],          # L2 regularization
}

# Set up randomized search
random_search = RandomizedSearchCV(estimator=xgb_model, param_distributions=param_grid, 
                                   n_iter=50, cv=5, scoring='neg_mean_squared_error', 
                                   verbose=2, random_state=42, n_jobs=-1)

# Fit the model
random_search.fit(X_train, y_train)

# Print the best parameters
print("Best parameters found: ", random_search.best_params_)

# Predict with the best model
best_model = random_search.best_estimator_

y_pred = best_model.predict(X_test)


y_test = np.array(y_test, dtype=np.float32)
y_pred = np.array(y_pred, dtype=np.float32)
y_test = np.nan_to_num(y_test)
y_pred = np.nan_to_num(y_pred)
print("y_test shape:", y_test.shape)
print("y_pred shape:", y_pred.shape)
# Evaluate performance
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
print(f'RMSE: {rmse}')
print(f'R²: {r2}')
print(f'MAE: {mae}')
print(f'MSE: {mse}')



# Create a DataFrame with actual and predicted values (since y is no longer scaled)
results_df = pd.DataFrame({'Actual': y_test.flatten(), 'Predicted': y_pred.flatten()})
print(results_df.head(30))


y_test_direction = np.sign(y_test.flatten())  # +1 for positive, -1 for negative
y_pred_direction = np.sign(y_pred.flatten())  # +1 for positive, -1 for negative

# Step 2: Calculate directional accuracy
correct_direction = np.sum(y_test_direction == y_pred_direction) / len(y_test_direction)
print(f"Directional Accuracy: {correct_direction * 100:.2f}%")

# Step 3: Confusion Matrix for directional prediction
conf_matrix = confusion_matrix(y_test_direction, y_pred_direction, labels=[1, -1])

print(f"Confusion Matrix:\n{conf_matrix}")

joblib.dump(best_model, 'model3.pkl')
# In your training code where you create label encoders:

# Important: Save the dictionary

# Remove this line from your original code:
# joblib.dump(le, 'label_encoders.pkl')  # This was incorrect

joblib.dump(scaler, 'x_scaler.pkl')
joblib.dump(y_scaler, 'y_scaler.pkl')



# joblib.dump(cols_to_drop, 'cols_to_drop.pkl')
joblib.dump(numerical_cols, 'numerical_cols.pkl')
joblib.dump(categorical_columns, 'categorical_columns.pkl')
joblib.dump(X_train.columns.tolist(), 'feature_names.pkl')

# Optionally, you can also save the results to a CSV for further analysis
#results_df.to_csv("C:\\Users\\dasantha\\Desktop\\combined_daily_data.csv\\predictions_vs_actuals.csv", index=False)