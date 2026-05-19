import json
import time
import random
from kafka import KafkaProducer

# 配置信息
TOPIC = "sensor-events"
BOOTSTRAP_SERVERS = "localhost:9092"  # 对应 docker-compose 中的 advertised.listeners

def run_producer(count=100, interval=1):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ 连接 Kafka 失败: {e}")
        print("请确保 Docker 容器正在运行 (docker ps)")
        return

    sensors = ["temperature", "humidity", "pressure"]
    print(f"🚀 开始发送 {count} 条模拟数据...")

    for i in range(count):
        data = {
            "id": i,
            "sensor": random.choice(sensors),
            "value": round(random.uniform(20, 100), 2),
            "timestamp": int(time.time() * 1000)
        }
        
        # 发送消息
        future = producer.send(TOPIC, value=data)
        
        # 同步确认（阻塞直到收到 ack，确保数据不丢）
        record_metadata = future.get(timeout=10)
        
        # 仅在调试时打印，大量数据时会卡死
        # print(f"✅ 发送至 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        time.sleep(interval) # 控制发送频率

    producer.close()
    print("🎉 发送完成！")

if __name__ == "__main__":
    run_producer(count=100, interval=0.5)