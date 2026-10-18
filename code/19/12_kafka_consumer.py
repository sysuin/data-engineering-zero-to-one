# compile-only: needs the confluent-kafka package and a Kafka broker
# The archive consumer of 05_duplicates.py, written for Kafka.
import duckdb
from confluent_kafka import Consumer, KafkaException

from groundwork.config import WAREHOUSE
from groundwork.stream.consumer import EventSink, parse

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "archive",             # the consumer group
    "enable.auto.commit": False,       # we commit, after the work
    "auto.offset.reset": "earliest",   # a new group starts at the start
    "isolation.level": "read_committed",
})
consumer.subscribe(["portal.events"])  # a topic: partitions of a log
sink = EventSink(duckdb.connect(str(WAREHOUSE)))

try:
    while True:
        batch = consumer.consume(num_messages=5000, timeout=1.0)
        if not batch:
            continue
        for message in batch:
            if message.error():
                raise KafkaException(message.error())
        # Each message carries its partition and offset; the value is
        # the event, byte for byte as the portal sent it.
        sink.write(parse(m.value()) for m in batch)   # work first
        consumer.commit(asynchronous=False)            # then commit
finally:
    consumer.close()
