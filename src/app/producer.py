from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from uuid import uuid4
from random import randint
from os import getenv
import json
import time

count = 1
SLEEP = 1
KAFKA_HOST=getenv('KAFKA_HOST')
KAFKA_PORT=getenv('KAFKA_PORT')
TOPIC=getenv('TOPIC')

def main():
    global count
    producer = KafkaProducer(
        bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
        value_serializer=lambda message: json.dumps(message).encode('utf-8')
    )
    while True:
        d = {'id': count, 'uuid': str(uuid4()), 'number': randint(1, 1000)}
        try:
            producer.send(TOPIC, d).get()
            print(f'sent: {d}')
            count = count + 1
            time.sleep(SLEEP)
        except KafkaTimeoutError as exc:
            print(f'Error sending the message to topic: {exc}')

if __name__ == '__main__':
    main()
