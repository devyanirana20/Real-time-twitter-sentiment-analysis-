from kafka import KafkaProducer # type: ignore
import time
import json
import random

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',  # Use host port mapped to Kafka container
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tweets = [
    "I love this product!",
    "This is the worst experience ever",
    "Feeling okay about today",
    "Absolutely amazing service!",
    "Not happy with the results",
    "This is so frustrating",
    "Best day ever!",
    "Could be better"
]

while True:
    tweet = random.choice(tweets)
    producer.send('test', {"text": tweet})
    print("Sent:", tweet)
    time.sleep(2)

# from kafka import KafkaProducer
# import json

# producer = KafkaProducer(
#     bootstrap_servers='127.0.0.1:9093',  # Use host port mapped to Kafka container
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# producer.send('test', {"text": "Hello from Python!"})
# producer.flush()
# print("Message sent!")