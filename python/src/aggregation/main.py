import os
import logging

from typing import TypeAlias

from common import middleware, fruit_item

from common.contracts import(
    Acknowledgment,
    ClientId,
    PartialResultMessage,
    PartialTotalMessage,
    SumFinishedMessage,
    serialize_partial_result_message,
    deserialize_sum_output_message,
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

FinishedCountByClient: TypeAlias = dict[ClientId, int]


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client = {}
        self.finished_sums_by_client: FinishedCountByClient = {}

    def _process_partial_total(self, partial_message: PartialTotalMessage) -> None:
        logging.info("Processing partial total message")

        amounts_by_fruit = self.amount_by_client.setdefault(partial_message.client_id, {})

        amounts_by_fruit[partial_message.fruit] = amounts_by_fruit.get(
            partial_message.fruit, fruit_item.FruitItem(partial_message.fruit, 0)
        ) + fruit_item.FruitItem(partial_message.fruit, partial_message.amount)

    def _process_sum_finished(self, finished_message: SumFinishedMessage) -> None:
        logging.info("Received SumFinishedMessage from sum %s", finished_message.sum_id)

        client_id = finished_message.client_id
        finished_count = self.finished_sums_by_client.get(client_id, 0) + 1
        self.finished_sums_by_client[client_id] = finished_count

        if finished_count < SUM_AMOUNT:
            return

        self.finished_sums_by_client.pop(client_id, None)
        amounts_by_fruit = self.amount_by_client.pop(client_id, {})
        ordered_fruits = sorted(amounts_by_fruit.values(), reverse=True)
        fruit_top = [
            (item.fruit, item.amount) for item in ordered_fruits[:TOP_SIZE]
        ]

        self.output_queue.send(
            serialize_partial_result_message(
                PartialResultMessage(
                    client_id=client_id,
                    aggregator_id=ID,
                    fruit_top=fruit_top
                )
            )
        )

    def process_messsage(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment)->None:
        try:
            logging.info("Process message")
            input_message = deserialize_sum_output_message(message)

            if isinstance(input_message, PartialTotalMessage):
                self._process_partial_total(input_message)
            else:
                self._process_sum_finished(input_message)
            ack()
        except Exception:
            logging.exception("No se pudo procesar un mensaje de Aggregation")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
