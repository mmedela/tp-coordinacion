import uuid

from common import message_protocol

from common.contracts import(
    ClientId,
    IngestionDataMessage,
    IngestionFinishedMessage,
    FruitTop,
    ResultMessage,
    deserialize_result_message as deserialize_result_contract,
    serialize_ingestion_data_message,
    serialize_ingestion_finished_message
)


class MessageHandler:

    def __init__(self):
        self.client_id: ClientId = str(uuid.uuid4())
        self.next_sequence = 0
    
    def serialize_data_message(self, message: tuple[str, str])->bytes:
        [fruit, amount] = message
        serializez_message = serialize_ingestion_data_message(
            IngestionDataMessage(
                client_id=self.client_id,
                sequence=self.next_sequence,
                fruit=fruit,
                amount=amount
            )
        )
        self.next_sequence += 1
        return serializez_message
    
    def serialize_eof_message(self, _message)->bytes:
        return serialize_ingestion_finished_message(
            IngestionFinishedMessage(
                client_id=self.client_id,
                record_count=self.next_sequence
            )
        )

    def deserialize_result_message(self, message)-> FruitTop | None:
        result_message: ResultMessage = deserialize_result_contract(message)

        if self.client_id != result_message.client_id:
            return None
        return result_message.fruit_top


