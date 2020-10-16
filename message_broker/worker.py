from message_broker.core import MessageBroker
import json
import logging

log = logging.getLogger(__name__)

class Worker:

    def __init__(self):
        self.broker = MessageBroker()
        self.broker.init_broker()
        self.broker.connect()
        self.channel = self.broker.create_channel()
        self.on_message_callback = None

    def on_message(self, channel, method, properties, body):
        try:
            body = body.decode()
            body = json.loads(body)
            self.on_message_callback(body)
        except Exception as error:
            log.info(f'Error ao processar a mensagem: {error.__doc__}')
            log.info(f'A mensagem permanecerá na fila!')

        channel.basic_ack(method.delivery_tag)

    def start(self, queue, callable):
        self.on_message_callback = callable
        self.broker.consumer(self.channel, queue, self.on_message)
