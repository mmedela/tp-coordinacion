import os
import logging

from common import middleware, message_protocol, fruit_item

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

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        amounts_by_fruit = self.amount_by_client.setdefault(client_id, {})

        amounts_by_fruit[fruit] = amounts_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        amounts_by_fruit = self.amount_by_client.pop(client_id, {})
        ordered_fruits = sorted(amounts_by_fruit.values(), reverse=True)
        fruit_top = [
            (item.fruit, item.amount) for item in ordered_fruits[:TOP_SIZE]
        ]
        
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        # del self.fruit_top

    def process_messsage(self, message, ack, nack):
        try:

            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            elif len(fields) == 1:
                self._process_eof(*fields)
            else:
                raise ValueError(f"Mensaje interno invalido {fields}")
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
