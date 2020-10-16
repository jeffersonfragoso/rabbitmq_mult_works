import logging
import json

from message_broker import MessageBroker

log = logging.getLogger(__name__)

class Producer:

    def __init__(self):
        self.broker = MessageBroker()
        self.broker.init_broker()
        self.broker.connect()
        self.channel = self.broker.create_channel()

    def setup(self):
        exchanges = [('service', 'fannout'), ('comparer', 'direct')]
        queues = ['123456789', '987654321']
        self.broker.setup(exchanges, queues)

    def send_message(self, exchange, message, routing_key=None):
        """
            Publica a mensagem no exchange

            Parameters:
            - exchange (str): Nome do exchange
            - message (dict): Mensagem que será publicada
            - routing_key (str): Chave de roteamento se necessário
        """

        message = json.dumps(message)
        self.broker.publish(self.channel, exchange, message, routing_key)
