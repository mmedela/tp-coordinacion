from __future__ import annotations

import zlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from common.message_protocol import internal
from common.fruit_item import FruitItem

ClientId: TypeAlias = str
FruitName: TypeAlias = str
Amount: TypeAlias = int
FruitRecord: TypeAlias = tuple[FruitName, Amount]
FruitTop: TypeAlias = list[FruitRecord]

SequenceNumber: TypeAlias = int
RecordCount: TypeAlias = int
SumId: TypeAlias = int
AggregatorId: TypeAlias = int

FruitTotals: TypeAlias = dict[FruitName, FruitItem]
ClientFruitTotals: TypeAlias = dict[ClientId, FruitTotals]

Acknowledgment: TypeAlias = Callable[[], None]

MessageConsumerCallback: TypeAlias = Callable[
    [bytes, Acknowledgment, Acknowledgment],
    None
]

_CONTROL_INGESTION_FINISHED = "ingestion_finished"

class InvalidInternalMessageError(ValueError):
    """El mensaje no cumple el contrato interno de la aplicaicon"""

@dataclass(frozen=True)
class ResultMessage:
    client_id: ClientId
    fruit_top: FruitTop

@dataclass(frozen=True)
class PartialResultMessage:
    client_id: ClientId
    aggregator_id: AggregatorId
    fruit_top: FruitTop

@dataclass(frozen=True)
class IngestionDataMessage:
    client_id: ClientId
    sequence: SequenceNumber
    fruit: FruitName
    amount: Amount

@dataclass(frozen=True)
class IngestionFinishedMessage:
    client_id: ClientId
    record_count: RecordCount

@dataclass(frozen=True)
class RecordProcessedMessage:
    client_id: ClientId
    sequence: SequenceNumber

@dataclass(frozen=True)
class FlushMessage:
    client_id: ClientId

@dataclass(frozen=True)
class PartialTotalMessage:
    client_id: ClientId
    fruit: FruitName
    amount: Amount

@dataclass(frozen=True)
class SumFinishedMessage:
    client_id: ClientId
    sum_id: SumId

@dataclass(frozen=True)
class IngestionReportedMessage:
    client_id: ClientId
    record_count: RecordCount

SumControlMessage: TypeAlias = (
    RecordProcessedMessage
    | IngestionReportedMessage
    | FlushMessage
)

IngestionMessage: TypeAlias = (
    IngestionDataMessage
    | IngestionFinishedMessage
)

SumOutputMessage: TypeAlias = (
    PartialTotalMessage
    | SumFinishedMessage
)

def aggregator_index_for_fruit(fruit: FruitName, aggregation_amount: int) -> AggregatorId:
    """Particion deterministica e independiente del proceso (no usa hash() de Python,
    que esta salteado por PYTHONHASHSEED y daria resultados distintos por replica)."""
    return zlib.crc32(fruit.encode("utf-8")) % aggregation_amount

def serialize_result_message(result_message: ResultMessage) -> bytes:
    return internal.serialize([
        result_message.client_id,
        result_message.fruit_top
    ])

def deserialize_result_message(message: bytes)->ResultMessage:
    fields = _deserialize_fields(message)

    if len(fields) != 2:
        raise InvalidInternalMessageError("Un resultado debe tener exactamente 2 campos")

    client_id, raw_fruit_top = fields

    return ResultMessage(
        client_id=_parse_client_id(client_id),
        fruit_top=_parse_fruit_top(raw_fruit_top)
    )

def serialize_partial_result_message(partial_result_message: PartialResultMessage) -> bytes:
    return internal.serialize([
        partial_result_message.client_id,
        partial_result_message.aggregator_id,
        partial_result_message.fruit_top
    ])

def deserialize_partial_result_message(message: bytes) -> PartialResultMessage:
    fields = _deserialize_exact_fields(
        message, 3, "Un resultado parcial debe tener exactamente 3 campos"
    )

    client_id, aggregator_id, raw_fruit_top = fields

    return PartialResultMessage(
        client_id=_parse_client_id(client_id),
        aggregator_id=_parse_non_negative_int(aggregator_id, "El id de aggregator"),
        fruit_top=_parse_fruit_top(raw_fruit_top)
    )

def _parse_fruit_top(raw_fruit_top: Any) -> FruitTop:
    if not isinstance(raw_fruit_top, list):
        raise InvalidInternalMessageError("El top debe ser una lista")

    fruit_top: FruitTop = []

    for raw_record in raw_fruit_top:
        if not isinstance(raw_record, list) or len(raw_record) != 2:
            raise InvalidInternalMessageError("Cada elemento del top debe ser un par (fruta, cantidad)")

        fruit, amount = raw_record
        fruit_top.append(
            (_parse_fruit(fruit), _parse_amount(amount))
        )

    return fruit_top


def serialize_ingestion_data_message(data_message: IngestionDataMessage)->bytes:
    return internal.serialize([
        data_message.client_id,
        data_message.sequence,
        data_message.fruit,
        data_message.amount
    ])


def deserialize_ingestion_data_message(message: bytes)->IngestionDataMessage:
    fields = _deserialize_exact_fields(
        message, 4, "un dato debe tener cuatro campos"
    )

    client_id, sequence, fruit, amount = fields

    return IngestionDataMessage(
        client_id=client_id,
        sequence=sequence,
        fruit=fruit,
        amount=amount
    )

def serialize_ingestion_finished_message(finished_message: IngestionFinishedMessage) -> bytes:
    return internal.serialize([
        finished_message.client_id,
        finished_message.record_count
    ])

def deserialize_ingestion_finished_message(message: bytes) -> IngestionFinishedMessage:
    fields = _deserialize_exact_fields(
        message, 2, "Un EOF de ingesta debe tener dos campos"
    )

    client_id, record_count = fields

    return IngestionFinishedMessage(
        client_id=_parse_client_id(client_id), 
        record_count=_parse_non_negative_int(record_count, "La cantidad de registros")
    )

def serialize_record_processed_message(processed_message: RecordProcessedMessage) -> bytes:
    return internal.serialize([
        processed_message.client_id,
        processed_message.sequence
    ])

def deserialize_record_processed_message(message: bytes) -> RecordProcessedMessage:
    fields = _deserialize_exact_fields(
        message, 2, "Una confirmacion de procesamiento debe tener 2 campos"
    )

    client_id, sequence = fields

    return RecordProcessedMessage(
        client_id=_parse_client_id(client_id),
        sequence=_parse_non_negative_int(sequence, "La sequencia")
    )

def serialize_flush_message(flush_message: FlushMessage) -> bytes:
    return internal.serialize([flush_message.client_id])

def deserialize_flush_message(message: bytes)->FlushMessage:

    fields = _deserialize_exact_fields(
        message, 1, "Un flush debe tener un unico campo"
    )

    return FlushMessage(client_id=_parse_client_id(fields[0]))

def serialize_partial_total_message(partial_total_message: PartialTotalMessage)->bytes:
    return internal.serialize([
        partial_total_message.client_id,
        partial_total_message.fruit,
        partial_total_message.amount
    ])

def deserialize_partial_total_message(message: bytes) -> PartialTotalMessage:

    fields = _deserialize_exact_fields(
        message, 3, "Un total parcial debe tener tres campos"
    )

    client_id, fruit, amount = fields

    return PartialTotalMessage(
        client_id=_parse_client_id(client_id),
        fruit=_parse_fruit(fruit),
        amount=_parse_amount(amount)
    )

def serialize_sum_finished_message(sum_finished_message: SumFinishedMessage)-> bytes:
    return internal.serialize([
        sum_finished_message.client_id,
        sum_finished_message.sum_id
    ])

def deserialize_sum_finished_message(message: bytes) -> SumFinishedMessage:

    fields = _deserialize_exact_fields(
        message, 2, "La finalizacion de Sum debe tener 2 campos"
    )

    client_id, sum_id = fields

    return SumFinishedMessage(
        client_id=_parse_client_id(client_id),
        sum_id=_parse_non_negative_int(sum_id, "El id de sum"),
    )


def serialize_ingestion_reported_message(ingestion_reported_message: IngestionReportedMessage) -> bytes:
    return internal.serialize([
        _CONTROL_INGESTION_FINISHED,
        ingestion_reported_message.client_id,
        ingestion_reported_message.record_count
    ])

def deserialize_ingestion_reported_message(message: bytes)->IngestionReportedMessage:

    fields = _deserialize_exact_fields(
        message, 3, "la notificacion de fin de ingesta debe tener 3 campos"
    )

    message_type, client_id, record_count = fields

    if message_type != _CONTROL_INGESTION_FINISHED:
        raise InvalidInternalMessageError("Mensaje de control desconocido")

    return IngestionReportedMessage(
        client_id=_parse_client_id(client_id),
        record_count=_parse_non_negative_int(record_count, "La cantidad de registros")
    )

def deserialize_sum_control_message(message:bytes)->SumControlMessage:
    fields = _deserialize_fields(message)

    if len(fields) == 1:
        return FlushMessage(client_id=_parse_client_id(fields[0]))

    if len(fields) == 2:
        client_id, sequence = fields
        return RecordProcessedMessage(
            client_id=_parse_client_id(client_id),
            sequence=_parse_non_negative_int(sequence, "La sequencia")
        )

    if len(fields) == 3:
        message_type, client_id, record_count = fields

        if message_type != _CONTROL_INGESTION_FINISHED:
            raise InvalidInternalMessageError("Mensaje de control desconocido")

        return IngestionReportedMessage(
            client_id=_parse_client_id(client_id),
            record_count=_parse_non_negative_int(record_count, "La cantidad de registros")
        )
    raise InvalidInternalMessageError("Mensaje de control de Sum invalido")

def deserialize_sum_output_message(message: bytes) -> SumOutputMessage:
    fields = _deserialize_fields(message)

    if len(fields) == 3:
        client_id, fruit, amount = fields
        return PartialTotalMessage(
            client_id=_parse_client_id(client_id),
            fruit=_parse_fruit(fruit),
            amount=_parse_amount(amount)
        )

    if len(fields) == 2:
        client_id, sum_id = fields
        return SumFinishedMessage(
            client_id=_parse_client_id(client_id),
            sum_id=_parse_non_negative_int(sum_id, "El id de sum"),
        )

    raise InvalidInternalMessageError("Un mensaje de salida de Sum debe tener 2 o 3 campos")

def deserialize_ingestion_message(message:bytes)->IngestionMessage:
    fields = _deserialize_fields(message)


    if len(fields) == 4:
        client_id, sequence, fruit, amount = fields
        return IngestionDataMessage(
            client_id=_parse_client_id(client_id),
            sequence=_parse_non_negative_int(sequence, "La sequencia"),
            fruit=_parse_fruit(fruit),
            amount=_parse_amount(amount)
        )

    if len(fields) == 2:
        client_id, record_count = fields

        return IngestionFinishedMessage(
            client_id=_parse_client_id(client_id),
            record_count=_parse_non_negative_int(record_count, "La cantidad de registros")
        )
    raise InvalidInternalMessageError("Un mensaje de ingesta debe tener 2 o 4 campos")
    
def _deserialize_fields(message: bytes) -> list[Any]:
    try:
        fields = internal.deserialize(message)
    except Exception as e:
        raise InvalidInternalMessageError(
            "No se pudo deserializar el mensaje interno"
        ) from e
    if not isinstance(fields, list):
        raise InvalidInternalMessageError("El mensaje interno debe ser una lista")
    return fields

def _parse_client_id(value: Any)->ClientId:
    if not isinstance(value, str) or not value:
        raise InvalidInternalMessageError("El id del cliente debe ser un str valido no vacio")
    return value

def _parse_fruit(value: Any)-> FruitName:
    if not isinstance(value, str) or not value:
            raise InvalidInternalMessageError("El nombre de la fruta debe ser un str valido no vacio")
    return value

def _parse_amount(value: Any)-> FruitName:
    if type(value) is not int:
            raise InvalidInternalMessageError("La cantidad debe ser un entero valido")
    return value

def _deserialize_exact_fields(message: bytes, expected_length: int, error_message: str)-> list[Any]:
    fields = _deserialize_fields(message)

    if len(fields) != expected_length:
        raise InvalidInternalMessageError(error_message)

    return fields

def _parse_non_negative_int(value: Any, field_name: str):
    if type(value) is not int or value < 0:
        raise InvalidInternalMessageError(f"{field_name} debe ser un entero no negativo")
    return value