import time
from flask import Flask, jsonify,request
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# MongoDB connection settings
mongo_uri = 'mongodb://192.168.43.197:27017/'
db_name = 'Telecom1'  # Specify your MongoDB database name
collection_name = 'prediction'  # Specify your MongoDB collection name

def get_latest_document():
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Find the latest document in the collection
    latest_document = collection.find_one(sort=[('_id', -1)], projection={'_id': 0})

    client.close()
    return latest_document

def get_total_document_count():
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Count all documents in the collection
        total_count = collection.count_documents({})

        client.close()
        return total_count
    except Exception as e:
        return str(e), 500

def calculate_percentage_change(current_value, previous_value):
    if previous_value != 0:
        percentage_change = ((current_value - previous_value) / previous_value) * 100
    else:
        percentage_change = 0  # Default to 0 if previous value is 0 (to avoid division by zero)
    return percentage_change
def calculate_percentage_of_total(part_value, total_value):
    if total_value != 0:
        percentage_of_total = (part_value / total_value) * 100
    else:
        percentage_of_total = 0  # Default to 0 if total value is 0 (to avoid division by zero)
    return percentage_of_total

@app.route('/')
def get_last_row():
    latest_document = get_latest_document()

    # Return the latest document as JSON response
    return jsonify(latest_document)

@app.route('/total')
def get_total():
    try:
        # Get current total document count
        current_total = get_total_document_count()

        # Example: Previous total (you can replace this with an actual previous value from your data)
        previous_total = current_total - 2  # Assuming previous value is current total minus 1000

        # Calculate percentage change
        percentage_change = calculate_percentage_change(current_total, previous_total)
        formatted_total = round(percentage_change, 1)
        total_value = current_total
        part_value = current_total//2
        percentage_of_total = calculate_percentage_of_total(part_value, total_value)

        # Prepare JSON response
        response_data = {
            'total': current_total,
            'percentage_change': formatted_total
        }

        return jsonify(response_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500




@app.route('/count_predictions', methods=['GET'])
def count_predictions():
   try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Count documents where 'prediction' field is equal to 0
        count = collection.count_documents({'prediction': 1})

        # Close MongoDB connection
        client.close()

        # Return the count of documents with prediction attribute == 0
        return jsonify(count=count), 200
   except Exception as e:
        return jsonify(error=str(e)), 500
   


@app.route('/count_predictions_staying', methods=['GET'])
def count_predictions_staying():
   try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Count documents where 'prediction' field is equal to 0
        count = collection.count_documents({'prediction': 0})

        # Close MongoDB connection
        client.close()

        # Return the count of documents with prediction attribute == 0
        return jsonify(count=count), 200
   except Exception as e:
        return jsonify(error=str(e)), 500
if __name__ == '__main__':
    app.run(debug=True, port=5000)

    # Continuously print the latest document every 5 seconds
    while True:
        time.sleep(5)
        print("Latest Document:")
        print(get_latest_document())