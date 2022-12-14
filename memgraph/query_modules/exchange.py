import mgp
import os
import json
from kafka import KafkaProducer

KAFKA_IP = os.getenv('KAFKA_IP', 'kafka')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')

@mgp.transformation
def prices(messages: mgp.Messages
           ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    # TODO implement your transformation module for Kafka
    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        message_json = json.loads(message.payload().decode('utf8'))

        # Rate will be the closing price, but it could be calculated differently
        rate = message_json['close']
        coin_base, coin_quote = message_json['pair'].split('_')

        result_queries.append(
            mgp.Record(
                query=("MERGE (base:Coin {exchange: $exchange, symbol: $coin_base})-[b:buy]->(quote:Coin {exchange: $exchange, symbol: $coin_quote}) "
                        "WITH base, quote, b "
                        "MERGE (quote)-[s:sell]->(base) "
                        "SET b.rate = ToFloat($buy_rate), s.rate = ToFloat($sell_rate)"
                ),
                parameters={
                    "exchange": message_json['exchange'],
                    "buy_rate": rate,
                    "sell_rate": 1/rate,
                    "coin_base": coin_base,
                    "coin_quote": coin_quote
                }
            )
        )

    return result_queries


@mgp.read_proc
def best_rate(context: mgp.ProcCtx,
                ) -> mgp.Record(best_rate=object):

    scoreboard = {  "buy": {"pair": "None", "exchange": "None", "rate": 0}, 
                    "sell": {"pair": "None", "exchange": "None", "rate": 0}}
    for coin_base in context.graph.vertices:
        exchange = coin_base.properties.get("exchange")
        base_symbol = coin_base.properties.get("symbol")

        for relation in coin_base.out_edges:
            rate = relation.properties.get("rate")
            type = relation.type.name

            coin_quote = relation.to_vertex
            quote_symbol = coin_quote.properties.get("symbol")

            if rate > scoreboard[type]["rate"]:
                scoreboard[type]["pair"] = base_symbol+"/"+quote_symbol
                scoreboard[type]["exchange"] = exchange
                scoreboard[type]["rate"] = rate
    
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_IP + ":" + KAFKA_PORT)
    kafka_producer.send(topic='best_rate', value = json.dumps(scoreboard).encode('utf-8'))

    return mgp.Record(best_rate=scoreboard)


@mgp.read_proc
def best_route(context: mgp.ProcCtx,
                exchange_: str,
                source_: str,
                destination_: str
                ) -> mgp.Record(best_route=list):

    graph_list = []

    for coin_base in context.graph.vertices:
        exchange = coin_base.properties.get("exchange")
        if exchange == exchange_:
            base_symbol = coin_base.properties.get("symbol")
            for relation in coin_base.out_edges:
                rate = relation.properties.get("rate")
                coin_quote = relation.to_vertex
                quote_symbol = coin_quote.properties.get("symbol")
                graph_list.append((base_symbol, quote_symbol, rate))
    
    dist, pred = {}, {}
    for s, d, w in graph_list:
        dist[s] = 0
    dist[source_] = 1

    for _ in range(len(graph_list) - 1):
        for s, d, w in graph_list:
            if dist[s] != 0:
                if dist[s] * w > dist[d]:
                    dist[d] = dist[s] * w
                    pred[d] = s


    if dist[destination_] == 0:
        return mgp.Record(best_route=['unavailable'])
    else:
        path = []
        current = destination_
        check = True
        while ( check ):
            check = (pred[current] != source_)
            path.append(current)
            current = pred[current]
        path.append(current)

        path.reverse()
    
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_IP + ":" + KAFKA_PORT)
    kafka_producer.send(topic='best_route', value = json.dumps(path).encode('utf-8'))

    return mgp.Record(best_route=path)
