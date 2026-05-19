# Sensor Data Stream Processing System

> Real-time sensor data collection, transmission, processing and analysis system


### Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Message Queue | Apache Kafka | 7.4.0 |
| Data Processing | Python | 3.8+ |
| Data Analysis | pandas | 2.2.0 |
| API Framework | Flask | 3.0.0 |
| Containerization | Docker | 20.10+ |

### Features

- ✅ Simulated sensor data generation (temperature, humidity, pressure)
- ✅ Kafka message queue transmission
- ✅ Real-time data processing and statistical analysis
- ✅ Anomaly detection and alerting
- ✅ RESTful API interface
- ✅ Containerized deployment

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Java 8/11 (required for Spark)

### Install Dependencies

```bash
cd "c:\Users\guan\Desktop\waike\Data engineer\exam"
pip install -r requirements.txt
```

### Start Services

```bash
# 1. Start Kafka container
docker-compose up -d

# 2. Wait 30 seconds for Kafka to start
# 3. Create Topic
python create_topic.py

# 4. Start data processor (new terminal)
python simple_processor.py

# 5. Start data producer (new terminal)
python src/producer.py

# Optional: Start Flask API (new terminal)
python src/api/app.py
```

### Stop Services

```bash
# Stop Docker containers
docker-compose down

# Press Ctrl+C to stop terminal programs
```

---

## 📁 Project Structure

```
.
├── src/                    # Source code directory
│   ├── api/               # REST API
│   │   └── app.py         # Flask application
│   ├── producer.py        # Data producer
│   ├── spark_pipeline.py  # Spark streaming processing (requires Java)
│   └── analytics.py       # Offline analytics queries
├── docs/                  # Documentation directory
│   ├── analytics.md       # Data stream analytics documentation
│   ├── fault_tolerance.md # Fault handling report
│   └── reflection.md      # Experiment reflection report
├── create_topic.py        # Kafka Topic creation script
├── simple_processor.py    # Simplified data processor (recommended)
├── docker-compose.yml     # Docker configuration
└── requirements.txt       # Python dependencies
```

---

## 📡 API Endpoints

### Health Check

```bash
GET /api/v1/health
```

**Response**:
```json
{"status": "ok"}
```

### Data Ingestion

```bash
POST /api/v1/readings
Content-Type: application/json

{"sensor": "temperature", "value": 25, "timestamp": 1234567890}
```

**Response**:
```json
{"message": "accepted"}
```

---

## 🔧 Configuration

### Kafka Configuration

| Parameter | Value |
|-----------|-------|
| Broker Address | `localhost:9092` |
| Topic Name | `sensor-events` |
| Partitions | 3 |
| Replication Factor | 1 |

### Anomaly Detection Rules

| Sensor Type | Normal Range | Anomaly Condition |
|-------------|--------------|-------------------|
| Temperature | - | > 35°C |
| Humidity | - | > 90% |
| Pressure | 990-1030 | < 990 or > 1030 |

---

## 📊 Sample Analytics Results

Based on test data (34 records):

| Sensor Type | Count | Anomaly Count | Anomaly Rate |
|-------------|-------|---------------|--------------|
| temperature | 9 | 5 | 55.6% |
| humidity | 13 | 6 | 46.2% |
| pressure | 12 | 0 | 0% |
| **Total** | **34** | **11** | **32.4%** |

---

## 🛠️ Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `NoBrokersAvailable` | Kafka not started | `docker-compose up -d` |
| Topic creation failed | Container not ready | Wait 30 seconds and retry |
| Spark startup failed | Java version incompatibility | Use `simple_processor.py` |

### View Logs

```bash
# Kafka container logs
docker logs kafka1

# Zookeeper container logs
docker logs zookeeper
```

---

## 📝 Documentation

- [Analytics Documentation](docs/analytics.md)
- [Fault Tolerance Report](docs/fault_tolerance.md)
- [Experiment Reflection](docs/reflection.md)

---

## 📄 License

MIT License

---

## 📧 Contact

For questions, please contact the project owner.