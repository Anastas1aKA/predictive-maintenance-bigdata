import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer

INTERVAL = int(os.getenv("INTERVAL", 30))

# ожидание Kafka
while True:
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        break
    except Exception:
        print("Kafka not ready, retrying...")
        time.sleep(5)

with open('/app/data/ai4i2020.csv', 'r') as f:
    lines = f.readlines()

# если есть заголовок — пропустить
lines = lines[1:]

current_index = 0
total_rows = len(lines)

while True:
    row = lines[current_index].strip().split(',')
    
    #увеличиваем счётчик и сбрасываем, если достигли конца
    current_index += 1
    if current_index >= total_rows:
        current_index = 0  # зацикливание: начинаем сначала

    # выбираем нужные столбцы
    filtered = [
        row[3],  # Air temperature
        row[4],  # Process temperature
        row[5],  # Rotational speed
        row[6],  # Torque
        row[7]   # Tool wear
    ]

    # добавляем дату и время
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filtered.append(current_time)

    msg = ','.join(filtered)

    producer.send('my-topic', msg.encode('utf-8'))
    print(f"Sent: {msg}")

    time.sleep(INTERVAL)