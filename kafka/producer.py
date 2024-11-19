from confluent_kafka import Producer
from kafka.settings import PRODUCER_CONFIG, CHUNK_SIZE
from kafka.helpers import delivery_report
import pandas as pd
import json
import time
import random

producer = Producer(PRODUCER_CONFIG)

def produce_msg(topic_name):
    try:
        chunks = pd.read_csv('data/credit_card_transactions-ibm_v2.csv', chunksize=CHUNK_SIZE)
        for chunk in chunks:
            for index, row in chunk.iterrows():
                row_dict = row.to_dict()
                transaction_data = json.dumps(row_dict, ensure_ascii=False)
                producer.produce(topic_name, value=transaction_data, callback=delivery_report)
                sleep_time = random.uniform(1, 3)
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        print('Stopping produce...')
    finally:
        producer.flush()
