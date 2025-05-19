
import pandas as pd
from flask import Flask, jsonify
import firebase_admin  
from firebase_admin import credentials, db
import json
import time  # For keeping script alive
from flask_cors import CORS
import pandas as pd
print(pd.__version__)  # This should print the pandas version if it's imported correctly

import numpy as np
from scipy import stats

from flask_cors import CORS

# Initialize Firebase Admin SDK
# cred = credentials.Certificate("C:\\Users\\dasantha\\Downloads\\stock-market-data-8947e-firebase-adminsdk-fbsvc-ac0cfbbf63.json")
cred = credentials.Certificate("cred.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://stock-market-data-8947e-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

# Initialize Flask
app = Flask(__name__)
CORS(app)  # Allow cross-origin requests

# Function to Process Data
def process_data(data):
    """Perform Data Analysis & Processing"""
    try:
        symbol = data.get("symbol")
        price = data.get("price")
        percentage_change = data.get("percentageChange")
        scrape_time = data.get("Scrape_time")

        # Simple analysis
        if price and percentage_change is not None:
            print(f"Processing Data: Symbol: {symbol}, Price: {price}, Change: {percentage_change}%")

            # Detect significant price changes
            if percentage_change > 5:
                print(f"🔥 Significant Price Jump detected for {symbol}: {price} ({percentage_change}%)")
        
            # Save processed data
            save_processed_data(data)

    except Exception as e:
        print(f"Error in Data Processing: {e}")

# Firebase Listener Function
def firebase_listener(event):
    """Handles new data added to Firebase"""
    try:
        json_data = event.data
        if json_data:
            process_data(json_data)  # Process Data
    except Exception as e:
        print(f"Error in Firebase Stream: {e}")

# Function to Stream Data from Firebase
def stream_firebase_data():
    """Attach Firebase Stream Listener"""
    ref = db.reference('/cse_data')
    ref.listen(firebase_listener)
    print("✅ Firebase Listener Started...")


# API Endpoint to Get Raw CSE Data
@app.route('/api/cse_data', methods=['GET'])
def get_cse_data():
    """Fetch raw CSE stock data from Firebase"""
    ref = db.reference('/cse_data')
    data = ref.get()
    
    if data:
        return jsonify(data), 200
    else:
        return jsonify({"message": "No CSE data found"}), 404

# API Endpoint to Get Processed Data
@app.route('/api/processed_data', methods=['GET'])
def get_processed_data():
    """Fetch processed CSE stock data from Firebase"""
    ref = db.reference('/processed_data')
    data = ref.get()
    
    if data:
        return jsonify(data), 200
    else:
        return jsonify({"message": "No processed data found"}), 404



@app.route('/api/analyzed_data', methods=['GET'])
def get_analyzed_data():
    """Fetch analyzed CSE stock data with trends and anomalies"""
    # Fetch raw data from Firebase
    ref = db.reference('/cse_data')
    raw_data = ref.get()

    if not raw_data:
        return jsonify({"message": "No data found"}), 404

    # Flatten JSON data
    flattened_data = []
    for timestamp, records in raw_data.items():
        for record in records:
            print(type(record))  # Debugging line to check the type of each record
            if isinstance(record, dict):
                record["timestamp"] = timestamp  # Add timestamp as a column
            else:
                # Handle cases where record is not a dictionary
                record = {"timestamp": timestamp, "data": record}
            flattened_data.append(record)

    df = pd.DataFrame(flattened_data)    # Convert to DataFrame

    print(df.head(3))  # Show first 3 rows
    print(df.shape)  # Check structure

    # Check if 'price' column exists, if not calculate it
    if 'price' not in df.columns:
        if 'Previous_Close_(Rs_)' in df.columns and 'Change(Rs)' in df.columns:
           df['price'] = df['Previous_Close_(Rs_)'] + df['Change(Rs)']

        else:
            return jsonify({"message": "Cannot calculate 'price': missing 'yesterday_price' or 'percentage_change'"}), 404

    # Perform data analysis
    df['price_change'] = df['price'].diff()  # Price change from previous record
    df['percentage_change'] = df['price_change'] / df['price'].shift(1) * 100

    # Detect anomalies using Z-score
    # Calculate z-scores only on valid percentage changes
    valid_pct_changes = df['percentage_change'].dropna()
    df['z_score'] = np.nan  # Create empty column with NaNs
    df.loc[valid_pct_changes.index, 'z_score'] = stats.zscore(valid_pct_changes)

# Then fill remaining NaNs
    df = df.fillna(0)
    df['anomaly'] = np.where(np.abs(df['z_score']) > 3, 'Anomaly', 'Normal')

    # Detect trends using a moving average
    df['moving_avg'] = df['price'].rolling(window=5).mean()

    df = df.fillna(0)
    # Convert analyzed data to JSON format
    analyzed_data = df.to_dict(orient='records')

    # Replace NaN values with null
    analyzed_data = json.loads(json.dumps(analyzed_data, default=lambda x: 0 if pd.isna(x) else x))



    return jsonify(analyzed_data), 200



# Start Firebase Streaming in a Background Thread
import threading
threading.Thread(target=stream_firebase_data, daemon=True).start()

# Run Flask App
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # Runs on port 5000
