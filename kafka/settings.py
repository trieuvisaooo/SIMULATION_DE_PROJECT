KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC_NAME = 'pos-transaction'
CHUNK_SIZE = 100000

# Producer Config
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'credit_card_transaction_producer',
    'batch.num.messages': 1000,
    'queue.buffering.max.messages': 50000
}
