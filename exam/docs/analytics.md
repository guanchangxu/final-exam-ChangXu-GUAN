# Sensor Data Stream Analytics Documentation

## 1. Project Overview

This project implements a real-time sensor data stream processing system based on Kafka + Python technology stack, enabling sensor data collection, transmission, processing, and analysis.

## 2. Architecture Design

### 2.1 System Architecture Diagram

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   Producer      │─────▶│    Kafka        │─────▶│   Processor     │
│  (Data Producer)│      │  (Message Queue)│      │ (Data Processor)│
└─────────────────┘      └─────────────────┘      └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
   Simulated Sensor Data    Message Buffer Storage    Real-time Analytics
    (Temperature/Humidity/Pressure)   (sensor-events)   (Anomaly Detection)
```

### 2.2 Component Description

| Component | Technology | Function Description |
|-----------|------------|---------------------|
| Producer | Python + kafka-python | Simulated sensor data generation and transmission |
| Kafka | Apache Kafka 7.4.0 | Message queue for data buffering and decoupling |
| Processor | Python + pandas | Real-time data processing, statistical analysis, anomaly detection |
| API | Flask | RESTful interface for external data access |

## 3. Data Stream Processing Flow

### 3.1 Data Collection Phase

The producer module simulates three types of sensor data:
- **Temperature Sensor**: Range 20-100°C
- **Humidity Sensor**: Range 20-100%
- **Pressure Sensor**: Range 20-100 (simulated value)

### 3.2 Data Transmission Phase

Kafka Topic Configuration:
- Topic Name: `sensor-events`
- Partitions: 3
- Replication Factor: 1

### 3.3 Data Processing Phase

The processor implements the following functions:
1. **Real-time Statistics**: Output statistics every 10 data points (min, max, average)
2. **Anomaly Detection**:
   - Temperature > 35°C is considered abnormal
   - Humidity > 90% is considered abnormal
   - Pressure < 990 or > 1030 is considered abnormal

## 4. Data Analysis Results

Based on test data (34 records):

| Sensor Type | Count | Anomaly Count | Anomaly Rate |
|-------------|-------|---------------|--------------|
| Temperature | 9 | 5 | 55.6% |
| Humidity | 13 | 6 | 46.2% |
| Pressure | 12 | 0 | 0% |
| **Total** | **34** | **11** | **32.4%** |

## 5. Technical Features

- **Real-time**: Data is processed immediately upon generation, with latency below 1 second
- **Scalability**: Kafka supports horizontal scaling for massive data streams
- **Fault Tolerance**: Kafka provides message persistence to ensure no data loss
- **Flexibility**: Processor logic can be flexibly extended to support new sensor types

## 6. Running Instructions

```bash
# 1. Start Kafka container
docker-compose up -d

# 2. Create Topic
python create_topic.py

# 3. Start processor (new terminal)
python simple_processor.py

# 4. Start producer (new terminal)
python src/producer.py
```