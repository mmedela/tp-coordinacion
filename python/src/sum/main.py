import os
import logging
import threading
from typing import TypeAlias

from common import middleware, message_protocol, fruit_item

from common.contracts import(
    Acknowledgment,
    ClientId,
    DataMessage,
    EndOfRecordsMessage,
    deserialize_data_or_eof_message,
    serialize_data_message,
    serialize_eof_message
)


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

AmountsByFruit: TypeAlias = dict[str, fruit_item.FruitItem]
AmountsByClient: TypeAlias = dict[ClientId, AmountsByFruit]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges: list[
            middleware.MessageMiddlewareExchangeRabbitMQ
        ] = []
        for aggregator_id in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{aggregator_id}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client: AmountsByClient = {}

    def _process_data(self, data_message: DataMessage)->None:
        logging.info(f"Process data")
        amounts_by_fruits = self.amount_by_client.setdefault(data_message.client_id, {})
        amounts_by_fruits[data_message.fruit] = amounts_by_fruits.get(
            data_message.fruit, fruit_item.FruitItem(data_message.fruit, 0)
        ) + fruit_item.FruitItem(data_message.fruit, data_message.amount)

    def _process_eof(self, eof_message: EndOfRecordsMessage)->None:
        logging.info(f"Broadcasting data messages")
        amounts_by_fruits = self.amount_by_client.pop(eof_message.Client_id, {})

        for final_fruit_item in amounts_by_fruits.values():
            output_message = serialize_data_message(
                DataMessage(
                    client_id=eof_message.Client_id,
                    fruit=final_fruit_item.fruit,
                    amount=final_fruit_item.amount
                )
            )
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(output_message)

        logging.info(f"Broadcasting EOF message")
        output_eof_message = serialize_eof_message(eof_message)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(output_eof_message)


    def process_data_messsage(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment):
        try:
            
            input_message = deserialize_data_or_eof_message(message)
            if isinstance(input_message, DataMessage):
                self._process_data(input_message)
            else:
                self._process_eof(input_message)
            ack()
        except Exception:
            logging.exception("No se pudo procesar el mensaje de Sum")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
