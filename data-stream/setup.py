import logging
from gqlalchemy import Memgraph, MemgraphKafkaStream, MemgraphTrigger
from gqlalchemy.models import TriggerEventType, TriggerEventObject, TriggerExecutionPhase
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep


log = logging.getLogger(__name__)


def connect_to_memgraph(memgraph_ip, memgraph_port):
    memgraph = Memgraph(host=memgraph_ip, port=int(memgraph_port))
    while True:
        try:
            if memgraph._get_cached_connection().is_active():
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)


def get_admin_client(kafka_ip, kafka_port):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_ip + ":" + kafka_port, client_id="data-stream"
            )
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            log.info("Failed to connect to Kafka")
            sleep(1)


def run(memgraph, kafka_ip, kafka_port):
    admin_client = get_admin_client(kafka_ip, kafka_port)
    log.info("Connected to Kafka")

    topic_list = [
        NewTopic(name="prices", num_partitions=1, replication_factor=1),
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    log.info("Created topics")

    log.info("Creating stream connections on Memgraph")
    stream = MemgraphKafkaStream(
        name="prices_stream",
        topics=["prices"],
        transform="exchange.prices",
        bootstrap_servers="'kafka:9092'",
    )
    memgraph.create_stream(stream)
    memgraph.start_stream(stream)

    best_rate_trigger = MemgraphTrigger(
            name="best_rate",
            event_type=TriggerEventType.UPDATE,
            event_object=TriggerEventObject.RELATIONSHIP,
            execution_phase=TriggerExecutionPhase.AFTER,
            statement="CALL exchange.best_rate() YIELD best_rate",
        )
    memgraph.create_trigger(best_rate_trigger)
