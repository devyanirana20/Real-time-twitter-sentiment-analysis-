print("🔥 THIS IS THE ACTIVE FILE")

from flask import Flask, jsonify, request
import happybase
from flask_cors import CORS
from kafka import KafkaProducer
import json

app = Flask(__name__)
CORS(app)

# 🔥 Kafka Producer
def get_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# 🔥 FETCH FROM HBASE (already exists)
def fetch_data():
    connection = happybase.Connection('localhost', port=9090)
    table = connection.table('sentiment_table')

    result = []

    for key, row in table.scan():
        result.append({
            "row": key.decode(),
            "text": row.get(b'data:text', b'').decode(),
            "sentiment": row.get(b'data:sentiment', b'').decode()
        })

    connection.close()
    return result

# 🔥 EXISTING ROUTE
@app.route('/data')
def data():
    return jsonify(fetch_data())

# 🚀 ADD THIS NEW ROUTE (THIS WAS MISSING)

@app.route('/send', methods=['POST'])
def send():
    data = request.json
    text = data.get("text")

    producer = get_producer()   # 👈 moved here

    producer.send('test', {"text": text})
    producer.flush()

    return jsonify({"status": "sent"})
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)