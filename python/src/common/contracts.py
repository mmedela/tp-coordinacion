from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from common.message_protocol import internal

ClientId: TypeAlias = str
FruitName: TypeAlias = str
Amount: TypeAlias = int
FruitRecord: TypeAlias = tuple[FruitName, Amount]
FruitTop: TypeAlias = list[FruitRecord]

Acknowledgment: TypeAlias = Callable[[], None]

MessageConsumerCallback: TypeAlias = Callable[
    [bytes, Acknowledgment, Acknowledgment],
    None
]

class InvalidInternalMessageError(ValueError):
    """El mensaje no cumple el contrato interno de la aplicaicon"""

@dataclass(frozen=True)
class DataMessage:
    client_id: ClientId
    fruit: FruitName
    amount: Amount

@dataclass(frozen=True)
class EndOfRecordsMessage:
    Client_id: ClientId

@dataclass(frozen=True)
class ResultMessage:
    client_id: ClientId
    fruit_top: FruitTop

def serialize_data_message(data_message: DataMessage) -> bytes:
    return internal.serialize([
        data_message.client_id,
        data_message.fruit,
        data_message.amount
    ])

def serialize_result_message(result_message: ResultMessage) -> bytes:
    return internal.serialize([
        result_message.client_id,
        result_message.fruit_top
    ])

def serialize_eof_message(eof_message: EndOfRecordsMessage) -> bytes:
    return internal.serialize([eof_message.Client_id])

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
        

def deserialize_data_or_eof_message(message: bytes) -> DataMessage | EndOfRecordsMessage:
    fields = _deserialize_fields(message)

    if len(fields) == 3:
        client_id, fruit, amount = fields
        return DataMessage(
            client_id=_parse_client_id(client_id),
            fruit=_parse_fruit(fruit),
            amount=_parse_amount(amount)
        )
    if len(fields) == 1:
        return EndOfRecordsMessage(
            Client_id=_parse_client_id(fields[0])
        )

    raise InvalidInternalMessageError("Un mensaje de entrada debe tener 1 o 3 campos")

def deserialize_result_message(message: bytes)->ResultMessage:
    fields = _deserialize_fields(message)

    if len(fields) != 2:
        raise InvalidInternalMessageError("Un resultado debe tener exactamente 2 campos")

    client_id, raw_fruit_top = fields

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

    return ResultMessage(
        client_id=_parse_client_id(client_id),
        fruit_top=fruit_top
    )

