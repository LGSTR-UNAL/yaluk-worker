import json
import time

from typing import Any
from confluent_kafka import Producer
from pandas import DataFrame

from . import lib_readPL4 as read_PL4

from typing import TypedDict, Any


class PayloadDict(TypedDict):
    simulation_id: str  # or UUID, depending on your code
    t: float
    measurement: str
    value: float


class DataDict(TypedDict):
    schema: Any  # Replace with the actual type of STROKE_SCHEMA if known
    payload: PayloadDict


STROKE_SCHEMA: Any = {
    "type": "struct",
    "name": "nowcast-linet-stroke.Value",
    "fields": [
        {"type": "string", "optional": False, "field": "simulation_id"},
        {"type": "double", "optional": False, "field": "t"},
        {"type": "string", "optional": False, "field": "measurement"},
        {"type": "float", "optional": False, "field": "value"},
    ],
}


def produce_results(
    pl4_path: str,
    uuid: str,
    producer: Producer,
    topic: str,
    flush_timeout: float = 60,
) -> None:
    A, B, C = read_PL4.readPL4(pl4_path)
    result: DataFrame = read_PL4.pl4_to_dataframe(A, B)

    for column in result.columns:
        data: DataDict = {
            "schema": STROKE_SCHEMA,
            "payload": {
                "simulation_id": uuid,
                "t": float(result[column][:-1].abs().idxmax()),
                "measurement": column,
                "value": float(result[column][:-1].abs().max()),
            },
        }
        producer.produce(topic=topic, value=json.dumps(data))
        time.sleep(0.1)  # Optional: Sleep to avoid overwhelming the Kafka broker
        producer.poll(0)
    pending = producer.flush(flush_timeout)
    if pending > 0:
        raise RuntimeError(f"{pending} result messages were not delivered to Kafka")
