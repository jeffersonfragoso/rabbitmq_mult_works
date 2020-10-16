import logging
import pika
from pika.exceptions import (AMQPConnectionError,
                             ConnectionClosed,
                             AMQPChannelError,
                             ChannelClosed,
                             UnroutableError,
                             AMQPError)

from pika.diagnostic_utils import create_log_exception_decorator

import socket
import time
import threading


log = logging.getLogger(__name__)
_log_exception = create_log_exception_decorator(log)

def catch_error(func):
    """Catch erros of rabbitmq"""

    connect_exceptions = (AMQPConnectionError,
                          ConnectionClosed,
                          AMQPChannelError,
                          ChannelClosed,
                          AMQPError,
                          socket.error)

    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except connect_exceptions as error:
            log.error(f'RabbitMQ error: {error.__doc__}')
            raise error

    return wrap


class MessageBroker():

    def __init__(self):
        self.channel = None
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.parameters = pika.ConnectionParameters('rabbit_server',
                                                    5672,
                                                    credentials=self.credentials,
                                                    connection_attempts=20,
                                                    retry_delay=5)
        self.publish_properties = pika.BasicProperties(delivery_mode=2)

    @catch_error
    def init_broker(self):
        lock = threading.Lock()
        thread = threading.Thread(target=self.connect, name='Conexao')
        try:
            with lock:
                thread.start()
        except Exception as error:
            log.exception(f'Erro ao iniciar o broker: {error}')

    @catch_error
    def connect(self):
        while True:
            try:
                self.connection = pika.BlockingConnection(self.parameters)
                log.info('Estabelecendo conexão.')
            except AMQPConnectionError as error:
                log.exception(f'Error ao abrir a conexão: {error}')

            if self.connection.is_open:
                return self.connection

    @catch_error
    def create_channel(self):
        if self.is_connected():
            self.channel = self.connection.channel()
            if self.channel.is_open:
                self.channel.confirm_delivery()
                return self.channel

    @catch_error
    def desconnect(self):
        try:
            self.channel.close()
            self.connection.close()
            log.info('Fechando conexão.')
        except AMQPError as error:
            log.exception(f'Error ao fechar a conexão: {error}')
            raise error

    @catch_error
    def is_connected(self, attempts=1000, time_between_attempts=0.01):
        while attempts:
            if self.connection.is_open and self.channel.is_open:
                log.info('Conexão estabelecida.')
                return True

            time.sleep(time_between_attempts)
            attempts -= 1

        log.info('A conexão não foi estabelecida')
        return False

    @catch_error
    def is_desconnected(self, attempts=1000, time_between_attempts=0.01):
        while attempts:
            if self.channel.is_closed and self.connection.is_closed:
                log.info('Conexão fechada.')
                return True

            time.sleep(time_between_attempts)
            attempts -= 1

        return False

    @catch_error
    def exchange(self, channel, name, type):
        if self.is_connected():
            channel.exchange_declare(exchange=name,
                                     type=type,
                                     passive=False,
                                     durable=True,
                                     auto_delete=False)

    @catch_error
    def queue(self, channel, name):
        if self.is_connected():
            channel.queue_declare(queue=name,
                                  durable=True)

    @catch_error
    def bind(self, channel, exchange, queue, routing_key=None):
        if self.is_connected():
            channel.queue_bind(exchange=exchange,
                               queue=queue,
                               routing_key=routing_key)

    @catch_error
    def publish(self, channel, exchange, body, router_key=None):
        if self.is_connected():
            try:
                channel.basic_publish(exchange=exchange,
                                      routing_key=router_key,
                                      body=body,
                                      mandatory=True,
                                      properties=self.publish_properties)
            except UnroutableError as error:
                log.exception(f'Não foi possível publicar no exchange {exchange}: {error.__doc__}')

    # Integrar com biblioteca retry
    @catch_error
    def consumer(self, channel, queue, callback):
        if self.is_connected():
            channel.basic_qos(prefetch_count=1)
            channel.basic_consumer(queue=queue,
                                   on_message_callback=callback)

            try:
                channel.start_consuming()
            except Exception as error:
                log.info(f'Error ao consumir a mensagem: {error.__doc__}')
                log.info(f'Retornando a mensagem par a fila.')
                channel.cancel()


    @catch_error
    def setup(self, exchanges, queues):

        for name, tipo in exchanges:
            self.exchange(self.channel, name, tipo)

            for queue in queues:
                if name=='service':
                    self.queue(self.channel, queue)
                    self.bind(self.channel, exchange=name,
                                            queue=queue,
                                            routing_key=queue)
                else:
                    self.queue(self.channel, f"{queue}.compare")
                    self.bind(self.channel, exchange=name,
                                            queue=f"{queue}.compare",
                                            routing_key=f"{queue}.compare")
