import os
import logging
import signal

from typing import TypeAlias

from common import middleware, fruit_item

from common.contracts import(
    Acknowledgment,
    ClientId,
    SumId,
    FruitName,
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

FinishedSumsByClient: TypeAlias = dict[ClientId, set[SumId]]
PartialKey: TypeAlias = tuple[ClientId, SumId, FruitName]


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client = {}
        self.finished_sums_by_client: FinishedSumsByClient = {}
        self.applied_partial_keys: set[PartialKey] = set()
        self.completed_clients: set[ClientId] = set()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame) -> None:
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()

    def _process_partial_total(self, partial_message: PartialTotalMessage) -> None:
        logging.info("Processing partial total message")

        client_id = partial_message.client_id
        if client_id in self.completed_clients:
            return

        partial_key: PartialKey = (client_id, partial_message.sum_id, partial_message.fruit)
        if partial_key in self.applied_partial_keys:
            logging.info("Total parcial duplicado de sum %s, descartando", partial_message.sum_id)
            return
        self.applied_partial_keys.add(partial_key)

        amounts_by_fruit = self.amount_by_client.setdefault(client_id, {})

        amounts_by_fruit[partial_message.fruit] = amounts_by_fruit.get(
            partial_message.fruit, fruit_item.FruitItem(partial_message.fruit, 0)
        ) + fruit_item.FruitItem(partial_message.fruit, partial_message.amount)

    def _process_sum_finished(self, finished_message: SumFinishedMessage) -> None:
        logging.info("Received SumFinishedMessage from sum %s", finished_message.sum_id)

        client_id = finished_message.client_id
        if client_id in self.completed_clients:
            return

        finished_sums = self.finished_sums_by_client.setdefault(client_id, set())
        if finished_message.sum_id in finished_sums:
            logging.info("SumFinishedMessage duplicado de sum %s, descartando", finished_message.sum_id)
            return
        finished_sums.add(finished_message.sum_id)

        if len(finished_sums) < SUM_AMOUNT:
            return

        self.completed_clients.add(client_id)
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
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
