from confluent_kafka import Consumer, KafkaError
from kafka.settings import TOPIC_NAME

# configure consumer
consumer_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'network-group',
    'auto.offset.reset': 'earliest' # read from beginning 
}

# init consumer
consumer = Consumer(consumer_config)

# subscribe topic
consumer.subscribe([TOPIC_NAME])

# process msg received
def process_message(msg):
    try:
        msg_content = msg.value().decode('utf-8')
        print(f"Received message: {msg_content}")
    except Exception as e:
        print(f"Error processing message: {e}")

# consume msg from topic
try:
    while 1:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f"Error: {msg.error()} \n")
        else:
            process_message(msg)      
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
