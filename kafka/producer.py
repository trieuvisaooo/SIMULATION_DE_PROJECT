from confluent_kafka import Producer
from kafka.settings import PRODUCER_CONFIG, CHUNK_SIZE
from kafka.helpers import delivery_report
import pandas as pd
import json
import time
import random
import os

producer = Producer(PRODUCER_CONFIG)

# File để lưu vị trí dòng cuối cùng đã đọc
OFFSET_FILE = 'offset.txt'

def get_last_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def produce_msg(topic_name):
    try:
        last_offset = get_last_offset()
        current_offset = 0

        chunks = pd.read_csv('data/credit_card_transactions-ibm_v2.csv', chunksize=CHUNK_SIZE)
        for chunk in chunks:
            for index, row in chunk.iterrows():
                current_offset += 1
                if current_offset <= last_offset:
                    continue  # Skip dòng đã gửi

                row_dict = row.to_dict()
                transaction_data = json.dumps(row_dict, ensure_ascii=False)
                producer.produce(topic_name, value=transaction_data, callback=delivery_report)
                save_offset(current_offset)  # Lưu offset sau khi gửi thành công

                sleep_time = random.uniform(1, 3)
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        print('Stopping produce...')
    finally:
        producer.flush()

