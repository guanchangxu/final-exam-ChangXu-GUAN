import json
from kafka import KafkaConsumer
import pandas as pd
import time

def run_processor():
    print("🔄 启动 Kafka 消费者...")
    
    consumer = KafkaConsumer(
        'sensor-events',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print("✅ 消费者启动成功！开始接收数据...")
    
    data_list = []
    start_time = time.time()
    
    for message in consumer:
        data = message.value
        data_list.append(data)
        
        # 每10条数据输出一次统计
        if len(data_list) % 10 == 0:
            df = pd.DataFrame(data_list[-10:])
            print(f"\n📊 最近10条数据统计:")
            print(f"   传感器类型: {df['sensor'].value_counts().to_dict()}")
            print(f"   值范围: {df['value'].min():.2f} ~ {df['value'].max():.2f}")
            print(f"   平均值: {df['value'].mean():.2f}")
            
            # 检测异常
            anomalies = df[(df['sensor'] == 'temperature') & (df['value'] > 35) |
                          (df['sensor'] == 'humidity') & (df['value'] > 90)]
            if len(anomalies) > 0:
                print(f"⚠️  发现 {len(anomalies)} 条异常数据！")
        
        # 运行30秒后停止
        if time.time() - start_time > 30:
            print("\n⏰ 处理完成！共接收", len(data_list), "条数据")
            break
    
    consumer.close()

if __name__ == "__main__":
    run_processor()