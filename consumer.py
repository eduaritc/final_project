from kafka import KafkaConsumer
from kafka import TopicPartition

consumer = KafkaConsumer("my_data_consumer")
for msg in consumer:
    print(msg)
