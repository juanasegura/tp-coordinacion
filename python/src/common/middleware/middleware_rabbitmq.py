import pika
from .middleware import (MessageMiddlewareQueue, MessageMiddlewareExchange,
                         MessageMiddlewareMessageError,
                         MessageMiddlewareDisconnectedError,
                         MessageMiddlewareCloseError)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError:
            if connection is not None:
                connection.close()
            raise MessageMiddlewareDisconnectedError()
        self.connection = connection
        self.channel = channel
        self.queue_name = queue_name
        self.is_consuming = False

    def start_consuming(self, on_message_callback):
        _start_consuming(self, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       body=message,
                                       properties=pika.BasicProperties(
                                           delivery_mode=pika.DeliveryMode.Persistent
                                       ))
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        self.stop_consuming()
        _close(self.connection)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            channel = connection.channel()
            channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for key in routing_keys:
                channel.queue_bind(exchange=exchange_name, queue=queue_name,
                                   routing_key=key)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()
        self.connection = connection
        self.channel = channel
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = queue_name
        self.is_consuming = False

    def start_consuming(self, on_message_callback):
        _start_consuming(self, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self)

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                                           routing_key=key, body=message,
                                           properties=pika.BasicProperties(
                                               delivery_mode=pika.DeliveryMode.Persistent
                                           ))
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        self.stop_consuming()
        _close(self.connection)


# funciones auxiliares para reducir código repetido -------------------------------------------------

def _start_consuming(middleware, on_message_callback):
    middleware.is_consuming = True

    def _handle_message(channel, method, properties, body):
        def ack():
            channel.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        on_message_callback(body, ack, nack)

    try:
        middleware.channel.basic_consume(queue=middleware.queue_name,
                                         on_message_callback=_handle_message)
        middleware.channel.start_consuming()
    except pika.exceptions.AMQPConnectionError:
        raise MessageMiddlewareDisconnectedError()
    except Exception:
        raise MessageMiddlewareMessageError()
    finally:
        middleware.is_consuming = False


def _stop_consuming(middleware):
    if not middleware.is_consuming:
        return
    try:
        middleware.channel.stop_consuming()
        middleware.is_consuming = False
    except pika.exceptions.AMQPConnectionError:
        raise MessageMiddlewareDisconnectedError()


def _close(connection):
    try:
        if connection.is_open:
            connection.close()
    except Exception:
        raise MessageMiddlewareCloseError()