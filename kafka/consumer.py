from kafka import KafkaConsumer

# Conexión al servidor Kafka
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='python-group'
)

print("📡 Esperando mensajes... (Ctrl+C para salir)")

for message in consumer:
    print(f"📥 Recibido: {message.value.decode('utf-8')}")
