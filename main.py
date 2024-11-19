from kafka import create_topic, produce_msg
from kafka.settings import TOPIC_NAME

def start_kafka():
    if create_topic(TOPIC_NAME):
        print('Start produce messages...')
        produce_msg(TOPIC_NAME)

if __name__ == '__main__':
    start_kafka()