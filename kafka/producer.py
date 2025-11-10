from kafka import KafkaProducer
import time

# Conexión al servidor Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

print("🚀 Enviando mensajes a Kafka...")

for i in range(5):
    mensaje = f"Hola desde Python #{i}"
    producer.send('test-topic', mensaje.encode('utf-8'))
    print(f"✅ Enviado: {mensaje}")
    time.sleep(1)  # Simula tiempo entre mensajes

producer.flush()
print("🎉 Envío completo.")
