import os
import logging

from common import middleware, message_protocol, fruit_item

from common.contracts import(
    Acknowledgment,
    ResultMessage,
    DataMessage,
    EndOfRecordsMessage,
    serialize_result_message,
    deserialize_data_or_eof_message,
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client = {}

    def _process_data(self, data_message: DataMessage)->None:
        logging.info("Processing data message")

        amounts_by_fruit = self.amount_by_client.setdefault(data_message.client_id, {})

        amounts_by_fruit[data_message.fruit] = amounts_by_fruit.get(
            data_message.fruit, fruit_item.FruitItem(data_message.fruit, 0)
        ) + fruit_item.FruitItem(data_message.fruit, data_message.amount)

    def _process_eof(self, eof_message: EndOfRecordsMessage)->None:
        logging.info("Received EOF")

        amounts_by_fruit = self.amount_by_client.pop(eof_message.Client_id, {})
        ordered_fruits = sorted(amounts_by_fruit.values(), reverse=True)
        fruit_top = [
            (item.fruit, item.amount) for item in ordered_fruits[:TOP_SIZE]
        ]
        
        self.output_queue.send(
            serialize_result_message(
                ResultMessage(
                    client_id=eof_message.Client_id,
                    fruit_top=fruit_top
                )
            )
        )
        # del self.fruit_top

    def process_messsage(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment)->None:
        try:

            logging.info("Process message")
            input_message = deserialize_data_or_eof_message(message)
            if isinstance(input_message, DataMessage):
                self._process_data(input_message)
            else:
                self._process_eof(input_message)
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
