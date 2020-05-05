from confluent_kafka import Consumer

# Initialise properties
bootstrap_servers_address = "localhost:9092"    # Change to your kafka server address
first_topic_name = "first_topic"    # Change to your topic
consumer_grou_id = "twitter"

# Create Kafka config dictionary
kafkaConsumerConfig = {
    'bootstrap.servers': bootstrap_servers_address,
    'group.id': consumer_grou_id,
    'auto.offset.reset': 'earliest'
}

# Initialise Consumer
consumer = Consumer(kafkaConsumerConfig)
consumer.subscribe([first_topic_name])

# Loop through messages
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        if msg.key():
            print(f' Key: {msg.key().decode("utf-8")}, message: {msg.value().decode("utf-8")}')
        else:
            print(f' Key: None, message: {msg.value().decode("utf-8")}')
        print(f' Partition: {msg.partition()}, offset: {msg.offset()}')
except KeyboardInterrupt:
    print("Stopped Consuming messages from Kafka")
finally:
    # Close consumer
    consumer.close()