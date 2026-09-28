import uuid

from common import message_protocol

from common.contracts import(
    ClientId,
    DataMessage,
    EndOfRecordsMessage,
    FruitTop,
    ResultMessage,
    deserialize_result_message,
    serialize_data_message,
    serialize_eof_message
)


class MessageHandler:

    def __init__(self):
        self.client_id: ClientId = str(uuid.uuid4())
    
    def serialize_data_message(self, message: tuple[str, str])->bytes:
        [fruit, amount] = message
        return serialize_data_message(
            DataMessage(
                client_id=self.client_id,
                fruit=fruit,
                amount=amount
            )
        )
    def serialize_eof_message(self, _message)->bytes:
        return serialize_eof_message(
            EndOfRecordsMessage(
                Client_id=self.client_id
            )
        )

    def deserialize_result_message(self, message)-> FruitTop | None:
        result_message: ResultMessage = deserialize_result_message(message)

        if self.client_id != result_message.client_id:
            return None
        return result_message.fruit_top



