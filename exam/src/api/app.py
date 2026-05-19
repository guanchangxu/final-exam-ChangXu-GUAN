from flask import Flask, request, jsonify
import json, time, random

app = Flask(__name__)

@app.get("/api/v1/health")
def health():
    return jsonify({"status": "ok"})

@app.post("/api/v1/readings")
def ingest():
    data = request.json
    return jsonify({"message": "accepted"}), 201

if __name__ == "__main__":
    app.run(debug=True)