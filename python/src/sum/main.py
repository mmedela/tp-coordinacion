import os
import logging
import threading
from typing import TypeAlias

from common import middleware, fruit_item

from common.contracts import(
    Acknowledgment,
    ClientId,
    ClientFruitTotals,
    IngestionDataMessage,
    IngestionFinishedMessage,
    RecordProcessedMessage,
    IngestionReportedMessage,
    FlushMessage,
    PartialTotalMessage,
    SumFinishedMessage,
    deserialize_ingestion_message,
    deserialize_sum_control_message,
    deserialize_flush_message,
    serialize_record_processed_message,
    serialize_ingestion_reported_message,
    serialize_flush_message,
    serialize_partial_total_message,
    serialize_sum_finished_message,
    aggregator_index_for_fruit,
)

COORDINATOR_SUM_ID = 0

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

SequencesByClient: TypeAlias = dict[ClientId, set[int]]
RecordCountsByClient: TypeAlias = dict[ClientId, int]


def _coordinator_queue_name() -> str:
    return f"{SUM_PREFIX}_coordinator"

def _flush_queue_name(sum_id: int)->str:
    return f"{SUM_PREFIX}_flush_{sum_id}"

class SumFilter:
    def __init__(self) -> None:
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.data_output_exchanges: list[
            middleware.MessageMiddlewareExchangeRabbitMQ
        ] = []

        for aggregator_id in range(AGGREGATION_AMOUNT):
            self.data_output_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{aggregator_id}"], producer_only=True
                )
            )

        self.coordinator_output_queue = ( middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, _coordinator_queue_name(),
        ))

        self.flush_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,  _flush_queue_name(ID),
        )

        self.coordinator_input_queue: (middleware.MessageMiddlewareQueueRabbitMQ | None) = None

        self.flush_output_queues: list[middleware.MessageMiddlewareQueueRabbitMQ] = []

        if ID == COORDINATOR_SUM_ID:
            self.coordinator_input_queue = (middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, _coordinator_queue_name(),
            ))

            for sum_id in range(SUM_AMOUNT):
                self.flush_output_queues.append(middleware.MessageMiddlewareQueueRabbitMQ(
                        MOM_HOST, _flush_queue_name(sum_id),
                ))

        self.state_lock = threading.Lock()

        self.amount_by_client: ClientFruitTotals = {}
        self.local_flushed_clients: set[ClientId] = set()

        self.reported_sequences_by_client: SequencesByClient = {}
        self.expected_records_by_client: RecordCountsByClient = {}
        self.coordinator_flushed_clients: set[ClientId] = set()


    def _handle_ingestion_data(self, data_message: IngestionDataMessage) -> None:
        logging.info(f"Start ingesting data of client {data_message.client_id}")
        with self.state_lock:
            totals = self.amount_by_client.setdefault(data_message.client_id, {})
            totals[data_message.fruit] = totals.get(
                data_message.fruit, fruit_item.FruitItem(data_message.fruit, 0)
            ) + fruit_item.FruitItem(data_message.fruit, data_message.amount)

        self.coordinator_output_queue.send(
            serialize_record_processed_message(
                RecordProcessedMessage(data_message.client_id, data_message.sequence)
            )
        )
    
    def _handle_ingestion_finished(self, finished_message: IngestionFinishedMessage) -> None:
            logging.info(f"Finish ingesting data of client {finished_message.client_id}")
            
            self.coordinator_output_queue.send(
                serialize_ingestion_reported_message(
                    IngestionReportedMessage(finished_message.client_id, finished_message.record_count)
                )
            )

    def _maybe_flush_locked(self, client_id: ClientId) -> None:
        if client_id in self.coordinator_flushed_clients:
            return
        expected = self.expected_records_by_client.get(client_id)
        if expected is None:
            return
        reported = self.reported_sequences_by_client.get(client_id, set())
        if reported != set(range(expected)):
            return

        self.coordinator_flushed_clients.add(client_id)
        self.reported_sequences_by_client.pop(client_id, None)
        self.expected_records_by_client.pop(client_id, None)

        flush_bytes = serialize_flush_message(FlushMessage(client_id))
        for flush_queue in self.flush_output_queues:
            flush_queue.send(flush_bytes)


    def process_ingestion_message(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment) -> None:
        try:
            ingestion_message = deserialize_ingestion_message(message)
            if isinstance(ingestion_message, IngestionDataMessage):
                self._handle_ingestion_data(ingestion_message)
            else:
                self._handle_ingestion_finished(ingestion_message)
            ack()
        except Exception:
            logging.exception("No se pudo procesar el mensaje de ingesta")
            nack()

    def process_coordinator_message(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment) -> None:
        try:
            control_message = deserialize_sum_control_message(message)

            if isinstance(control_message, FlushMessage):
                logging.warning("Mensaje de flush inesperado en la cola coordinadora")
                ack()
                return

            client_id = control_message.client_id

            with self.state_lock:
                if isinstance(control_message, RecordProcessedMessage):
                    self.reported_sequences_by_client.setdefault(client_id, set()).add(control_message.sequence)
                elif isinstance(control_message, IngestionReportedMessage):
                    self.expected_records_by_client[client_id] = control_message.record_count

                self._maybe_flush_locked(client_id)
            ack()
        except Exception:
            logging.exception("No se pudo procesar el mensaje de coordinacion")
            nack()

    def process_flush_message(self, message: bytes, ack: Acknowledgment, nack: Acknowledgment) -> None:
        try:
            flush_message = deserialize_flush_message(message)
            self._handle_flush(flush_message.client_id)
            ack()
        except Exception:
            logging.exception("No se pudo procesar el flush")
            nack()


    def _handle_flush(self, client_id: ClientId) -> None:
        with self.state_lock:
            if client_id in self.local_flushed_clients:
                return
            self.local_flushed_clients.add(client_id)
            totals = self.amount_by_client.pop(client_id, {})

        for final_fruit_item in totals.values():
            aggregator_index = aggregator_index_for_fruit(final_fruit_item.fruit, AGGREGATION_AMOUNT)
            self.data_output_exchanges[aggregator_index].send(
                serialize_partial_total_message(
                    PartialTotalMessage(client_id, final_fruit_item.fruit, final_fruit_item.amount)
                )
            )

        finished_bytes = serialize_sum_finished_message(SumFinishedMessage(client_id, ID))
        for output_exchange in self.data_output_exchanges:
            output_exchange.send(finished_bytes)

    def start(self) -> None:
        threads = [
            threading.Thread(target=self.input_queue.start_consuming, args=(self.process_ingestion_message,), daemon=True),
            threading.Thread(target=self.flush_input_queue.start_consuming, args=(self.process_flush_message,), daemon=True),
        ]
        if self.coordinator_input_queue is not None:
            threads.append(
                threading.Thread(target=self.coordinator_input_queue.start_consuming, args=(self.process_coordinator_message,), daemon=True)
            )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
