import os
import logging
import signal
from typing import TypeAlias

from common import middleware, fruit_item

from common.contracts import(
    Acknowledgment,
    ClientId,
    AggregatorId,
    FruitTop,
    ResultMessage,
    PartialResultMessage,
    serialize_result_message,
    deserialize_partial_result_message,
)

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

PartialResultsByAggregator: TypeAlias = dict[AggregatorId, FruitTop]
PartialResultsByClient: TypeAlias = dict[ClientId, PartialResultsByAggregator]

class JoinFilter:

    def __init__(self)->None:
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_results_by_client: PartialResultsByClient = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame) -> None:
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

    def _handle_partial_result(self, partial_message: PartialResultMessage) -> None:
        client_results = self.partial_results_by_client.setdefault(partial_message.client_id, {})
        client_results[partial_message.aggregator_id] = partial_message.fruit_top

        if len(client_results) < AGGREGATION_AMOUNT:
            return

        self.partial_results_by_client.pop(partial_message.client_id, None)

        merged_items = [
            fruit_item.FruitItem(fruit, amount)
            for fruit_top in client_results.values()
            for fruit, amount in fruit_top
        ]
        ordered_items = sorted(merged_items, reverse=True)
        fruit_top = [(item.fruit, item.amount) for item in ordered_items[:TOP_SIZE]]

        self.output_queue.send(
            serialize_result_message(
                ResultMessage(
                    client_id=partial_message.client_id,
                    fruit_top=fruit_top
                )
            )
        )

    def process_messsage(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment)->None:
        try:
            partial_message = deserialize_partial_result_message(message)
            self._handle_partial_result(partial_message)
            ack()
        except Exception:
            logging.exception("No se pudo procesar un mensaje en Join")
            nack()

    def start(self)->None:
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main()->int:
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()