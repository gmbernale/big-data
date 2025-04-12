from kafka import KafkaProducer
import random
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

event_types = ['login', 'purchase', 'logout', 'view', 'click']

while True:
    # Generar evento aleatorio
    user_id = random.randint(1, 1000)
    event = random.choice(event_types)
    timestamp = int(time.time() * 1000)
    
    # Crear mensaje en formato JSON
    message = f'{{"user_id": {user_id}, "event": "{event}", "timestamp": {timestamp}}}'
    
    # Enviar mensaje al topic
    producer.send('eventos-tiempo-real', message.encode('utf-8'))
    print(f"Enviado: {message}")
    time.sleep(random.uniform(0.1, 1.0))
