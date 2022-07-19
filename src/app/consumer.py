from kafka import KafkaConsumer
from os import getenv

KAFKA_HOST=getenv('KAFKA_HOST')
KAFKA_PORT=getenv('KAFKA_PORT')
TOPIC=getenv('TOPIC')

consumer = KafkaConsumer(TOPIC, bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'], auto_offset_reset='earliest')
consumer.subscribe(topics=['sample'])
for message in consumer:
    print(message)
