from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import time

print("🔄 正在尝试连接 Kafka 并创建 Topic...")

admin_client = None
try:
    # 1. 建立客户端 (增加超时容忍度)
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="topic-creator",
        request_timeout_ms=30000,  # 这里把超时时间设为 30 秒，防止卡死
        retry_backoff_ms=1000      # 失败后的重试间隔
    )

    # 2. 定义要创建的 Topic
    topic_list = []
    topic_list.append(NewTopic(name="sensor-events", num_partitions=3, replication_factor=1))
    
    # 3. 执行创建
    print("⚡ 正在发送创建请求...")
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("✅ 成功！Topic 'sensor-events' 已创建。")

except KafkaError as e:
    print(f"❌ 发生错误: {e}")
    print("💡 请检查 Kafka 容器是否正在运行 (docker-compose up -d)")
finally:
    if admin_client:
        admin_client.close()