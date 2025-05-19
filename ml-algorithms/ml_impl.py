import pandas as pd
import xgboost as xgb
import joblib
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from flask import Flask, jsonify
import traceback
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
# Load models and preprocessing objects once at app startup
model = joblib.load('model3.pkl')
scaler = joblib.load('x_scaler.pkl')
label_encoders = joblib.load('label_encoders4.pkl')
allowed_categories = joblib.load('allowed_categories3.pkl')

print("Model expects features:", model.feature_names_in_)
print("Number of features:", len(model.feature_names_in_))

def parse_date(date_value):
    """Handle multiple date formats including integer and string versions"""
    date_str = str(date_value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def fetch_and_preprocess_live_data():
    # Load financial data
    finance_df = pd.read_csv("outputNew_cleaned2.csv").rename(columns={
        'Symbol': 'symbol'  # Convert to lowercase
    })
    finance_df = finance_df.assign(
        Total_Revenue=finance_df['Total_Revenue'].astype(float).fillna(0),
        Net_Income=finance_df['Net_Income'].astype(float).fillna(0),
        Basic_EPS=finance_df['Basic_EPS'].astype(float).fillna(0)
    )

    # Fetch live data
    response = requests.get("http://localhost:5000/api/analyzed_data")
    live_data = json.loads(response.text)

    processed_data = []
    for item in live_data:
        original_date = parse_date(item.get("Date"))
        if not original_date:
            continue

        symbol = item.get("Symbol")
        name = item.get("Company_Name")

        new_item = {
            "symbol": symbol,
            "name": name,
            "date": original_date.strftime("%Y-%m-%d"),
            "price": float(item.get("**Last_Trade_(Rs_)", 0)),
            "tradevolume": float(item.get("Trade_Volume", 0)),
            "sharevolume": float(item.get("Share_Volume", 0))
        }
        processed_data.append(new_item)

    # Convert to DataFrame
    live_df = pd.DataFrame(processed_data)

    # Merge with financial data
    merged_df = pd.merge(live_df, finance_df, on="symbol", how="left")
    return merged_df

def calculate_technical_indicators(df):
    df = df.sort_values(by=['symbol', 'date']).reset_index(drop=True)
    
    # Lag features
    for lag in [1, 2, 3, 5, 7]:
        df[f'price_lag_{lag}'] = df.groupby('symbol')['price'].shift(lag)
    
    # Momentum and moving averages
    df['momentum_3'] = df['price'] - df.groupby('symbol')['price'].shift(3)
    df['mom5'] = df['price'] / df.groupby('symbol')['price'].shift(5) - 1
    for window in [5, 10]:
        df[f'ma{window}'] = df.groupby('symbol')['price'].rolling(window).mean().reset_index(level=0, drop=True)
    
    # Volatility and volume features
    df['volatility14'] = df.groupby('symbol')['price'].rolling(14).std().reset_index(level=0, drop=True)
    df['vol_ma5'] = df.groupby('symbol')['tradevolume'].rolling(5).mean().reset_index(level=0, drop=True)
    df['volume_spike'] = (df['tradevolume'] > 1.5 * df['vol_ma5']).astype(int)
    
    # MACD
    df['ema12'] = df.groupby('symbol')['price'].rolling(12, min_periods=1).mean().reset_index(level=0, drop=True)
    df['ema26'] = df.groupby('symbol')['price'].rolling(26, min_periods=1).mean().reset_index(level=0, drop=True)
    df['MACD'] = df['ema12'] - df['ema26']
    
    # RSI
    df['RSI'] = df.groupby('symbol')['price'].transform(lambda x: calculate_rsi(x))
    
    # Financial ratios
    df["Book_Value"] = df["Total_Revenue"] / df["sharevolume"]
    df["Profit_Margin"] = df["Net_Income"] / df["Total_Revenue"].replace(0, np.nan)
    df["Profit_Margin"] = df["Profit_Margin"].fillna(0)
    
    return df

def calculate_rsi(price_series, window=14):
    delta = price_series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window, min_periods=1).mean()
    avg_loss = loss.rolling(window, min_periods=1).mean()

    rs = avg_gain / (avg_loss + 1e-6)
    return 100 - (100 / (1 + rs))

def process_live_data():
    try:
        live_df = fetch_and_preprocess_live_data()
        if live_df.empty:
            return pd.DataFrame()

        # Filter valid symbols
        valid_symbols = allowed_categories['symbol']
        filtered_df = live_df[live_df['symbol'].isin(valid_symbols)].copy()
        if filtered_df.empty:
            return pd.DataFrame()

        # Categorical encoding
        for col_name, le in label_encoders.items():
            known_labels = set(le.classes_)
            filtered_df = filtered_df[filtered_df[col_name].isin(known_labels)]
            if filtered_df.empty:
                return pd.DataFrame()
            filtered_df.loc[:, col_name] = le.transform(filtered_df[col_name])

        # Calculate indicators
        processed_df = calculate_technical_indicators(filtered_df)
        
        # Feature selection and scaling
        feature_cols = model.feature_names_in_.tolist()
        feature_df = processed_df[feature_cols].fillna(0)
        scaled_features = scaler.transform(feature_df)
        
        # Predictions
        predictions = model.predict(scaled_features)
        processed_df['predicted_change'] = predictions
        processed_df['predicted_price'] = processed_df['price'] * (1 + processed_df['predicted_change'])
        
        # Add prediction date (next trading day)
        processed_df['date'] = pd.to_datetime(processed_df['date'])
        processed_df['prediction_date'] = processed_df['date'] + pd.DateOffset(days=1)
        
        # Filter future predictions
        current_date = datetime.now()
        processed_df = processed_df[processed_df['prediction_date'] > current_date]
        
        # Format dates
        processed_df['date'] = processed_df['date'].dt.strftime('%Y-%m-%d')
        processed_df['prediction_date'] = processed_df['prediction_date'].dt.strftime('%Y-%m-%d')

        if not processed_df.empty:
            # Reverse label encoding for symbols
            if 'symbol' in label_encoders:
                le = label_encoders['symbol']
                try:
                    processed_df['symbol'] = le.inverse_transform(processed_df['symbol'].astype(int))
                except Exception as e:
                    print(f"Error reversing symbol encoding: {str(e)}")
        
        return processed_df[['symbol', 'prediction_date', 'predicted_price', 'predicted_change']]
        
       
    
    except Exception as e:
        traceback.print_exc()
        return pd.DataFrame()

@app.route("/predict", methods=["GET"])
def predict():
    try:
        predictions_df = process_live_data()
        if predictions_df.empty:
            return jsonify({
                "status": "success",
                "data": [],
                "message": "No future predictions available"
            })
        
        # Convert to frontend format
        predictions_df = predictions_df.sort_values("predicted_change", ascending=False)
        predictions_df['predicted_change'] = predictions_df['predicted_change'].round(4) * 100  # Convert to percentage
        
        return jsonify({
            "status": "success",
            "data": {
                "symbols": predictions_df['symbol'].unique().tolist(),
                "predictions": predictions_df.to_dict(orient='records')
            }
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Prediction failed: {str(e)}"
        })
    

@app.route("/api/predict", methods=["POST"])
def api_predict():
    try:
        # Optionally log or parse incoming JSON data
        request_data = request.get_json()
        print("Received POST request with data:", request_data)

        predictions_df = process_live_data()
        if predictions_df.empty:
            return jsonify({
                "status": "success",
                "data": [],
                "message": "No future predictions available"
            })

        predictions_df = predictions_df.sort_values("predicted_change", ascending=False)
        predictions_df['predicted_change'] = predictions_df['predicted_change'].round(4) * 100  # As percentage

        return jsonify({
            "status": "success",
            "data": {
                "symbols": predictions_df['symbol'].unique().tolist(),
                "predictions": predictions_df.to_dict(orient='records')
            }
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": f"Prediction failed: {str(e)}"
        })

if __name__ == "__main__":
    app.run(debug=True, port=8000)



