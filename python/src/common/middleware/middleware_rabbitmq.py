import pika
from pika.adapters.blocking_connection import BlockingChannel
import  pika.spec as PikaSpec
import pika.exceptions as PikaExceptions
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange
from common.utils.exception_checker import ExceptionChecker

_PIKA_ERROR_MAPPINGS = (
    ((PikaExceptions.AMQPConnectionError, PikaExceptions.ChannelWrongStateError, ConnectionError, OSError), MessageMiddlewareDisconnectedError),
    (PikaExceptions.AMQPError, MessageMiddlewareMessageError)
)

class _RabbitMQMiddlewareBase:
    def __init__(self, connection: pika.BlockingConnection, channel: BlockingChannel):
        self.connection = connection
        self.channel = channel

    def start_consuming(self, queue_name: str, on_message_callback):
        def _on_message(ch: BlockingChannel, method:  PikaSpec.Basic.Deliver, _properties: PikaSpec.BasicProperties, body: bytes):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(body, ack, nack)

        with ExceptionChecker(*_PIKA_ERROR_MAPPINGS):

            #Necesito auto_ack=False, para que se llame a las funciones ack y nack. En la version actual
            #es False por defecto. 
            self.channel.basic_consume(queue=queue_name, on_message_callback=_on_message)
            self.channel.start_consuming()
        

    def stop_consuming(self):
            with ExceptionChecker(*_PIKA_ERROR_MAPPINGS):
                self.channel.stop_consuming()
    
    def close(self):
        with ExceptionChecker(*_PIKA_ERROR_MAPPINGS):
            self.channel.close()
            self.connection.close()
    

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name:str = queue_name

        self.connection:pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel:BlockingChannel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        self._base = _RabbitMQMiddlewareBase(connection=self.connection, channel=self.channel)

    def send(self, message):
        with ExceptionChecker(*_PIKA_ERROR_MAPPINGS):
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
       

    def start_consuming(self, on_message_callback):
        self._base.start_consuming(queue_name=self.queue_name, on_message_callback=on_message_callback)

    def stop_consuming(self):
        self._base.stop_consuming()

    def close(self):
       self._base.close()
        
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host: str, exchange_name: str, routing_keys: list[str],  producer_only: bool = False):
        self.exchange_name:str = exchange_name
        self.routing_keys:list[str] = routing_keys
        self.producer_only: bool = producer_only
        self.connection:pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel:BlockingChannel = self.connection.channel()
        #Direct porque hay tests con casos "direct messaging" (1 key -> 1 consumidor) y otros
        #tipo "broadcas" (1 key -> muchos consumidores). Permite ambos casos. fanout ignoraria la key
        #y topic no es necesaria para los casos de uso
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        self.queue: str | None = None

        if not self.producer_only:
            declared_queue = self.channel.queue_declare(queue='', exclusive=True)
            self.queue = declared_queue.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue, routing_key=routing_key)

        self._base = _RabbitMQMiddlewareBase(connection=self.connection, channel=self.channel)


        

    def send(self, message):
        with ExceptionChecker(*_PIKA_ERROR_MAPPINGS):
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)


    def start_consuming(self, on_message_callback):
        if self.producer_only or self.queue is None:
            raise MessageMiddlewareMessageError("Un exchange configurado como publisher no puede consumir")
        self._base.start_consuming(queue_name=self.queue, on_message_callback=on_message_callback)

    def stop_consuming(self):
            self._base.stop_consuming()
    
    def close(self):
        self._base.close()
