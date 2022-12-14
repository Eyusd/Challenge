import json
import os
from argparse import ArgumentParser
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from time import sleep
import setup

from fnmatch import fnmatch
from collections import defaultdict


KAFKA_IP = os.getenv('KAFKA_IP', 'kafka')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
MEMGRAPH_IP = os.getenv('MEMGRAPH_IP', 'memgraph-mage')
MEMGRAPH_PORT = os.getenv('MEMGRAPH_PORT', '7687')


def parse_args():
    """
    Parse input command line arguments.
    """
    parser = ArgumentParser(
        description="An exchange stream machine.")
    parser.add_argument("--data-directory", help="Directory with prices data.")
    parser.add_argument(
        "--interval",
        type=int,
        help="Interval for sending data in seconds.")
    return parser.parse_args()


def create_kafka_producer():
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_IP + ':' + KAFKA_PORT)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)


def main():
    args = parse_args()
                    
    memgraph = setup.connect_to_memgraph(MEMGRAPH_IP, MEMGRAPH_PORT)
    setup.run(memgraph, KAFKA_IP, KAFKA_PORT)

    producer = create_kafka_producer()
    
    # TODO push data to the Kafka topic
    ordered_data = defaultdict(list)
    for path, subdirs, files in os.walk(args.data_directory):
        for name in files:
            if fnmatch(name, "*.json"):
                pair = name.split('-')[0]
                exchange = path.split("/")[-1]

                with open(os.path.join(path, name)) as f:
                    data = json.load(f)
                    for row in data:
                        # Simulate realtime series, sorting data by timestamp
                        formatted_elt = {'exchange': exchange, 'pair': pair, 'open': row[1],  'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]}
                        ordered_data[row[0]].append(formatted_elt)

    for ts, current_market in ordered_data.items():
        for elt in current_market:
            producer.send(topic='prices', value = json.dumps(elt).encode('utf-8'))
        producer.flush()
        sleep(args.interval)

if __name__ == "__main__":
    print(KAFKA_IP, KAFKA_PORT, MEMGRAPH_IP, MEMGRAPH_PORT)
    main()
