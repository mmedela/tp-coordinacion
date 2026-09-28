import os
import logging
from collections.abc import Callable
from typing import Any

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

Acknowledgment = Callable[[], None]

class JoinFilter:

    def __init__(self)->None:
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment)->None:
        logging.info("Received top")
        try:
            fields: list[Any] = message_protocol.internal.deserialize(message)
            if len(fields) != 2:
                raise ValueError(f"Mensaje interno invalido: {fields}")
            client_id, fruit_top = fields
            if not (isinstance(client_id, str) and isinstance(fruit_top, list)):
                raise ValueError(f"Resultado invalido: {fields}")

            self.output_queue.send(
                message_protocol.internal.serialize([client_id, fruit_top])
            )
            ack()
        except Exception:
            logging.exception("No se pudo procesar un mensaje en Join")
            nack()

    def start(self)->None:
        self.input_queue.start_consuming(self.process_messsage)


def main()->int:
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()