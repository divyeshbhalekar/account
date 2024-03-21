import os
import json
import pika
import logging
import time
import app
from pika.exchange_type import ExchangeType
# from app.services.save_db import ACCOUNTSERVICE
from app.services.update_db import process_message
import asyncio

FORMAT = '%(asctime)s\t| %(levelname)s | line %(lineno)d, %(name)s, func: %(funcName)s()\n%(message)s\n'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger("consumer")
logger.setLevel(level=logging.INFO)

class Consumer:
    def __init__(self, amqp_url):
        self._url = amqp_url
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None

    def connect(self):
        queue=os.getenv('RABBITMQ_QUEUE','accounting')
        exchange=os.getenv('EXCHANGE_NAME','accounting')
        logger.info('Connecting to %s', self._url)
        parameters = pika.URLParameters(self._url)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self.setup_exchange_and_queue(queue,exchange)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=queue, on_message_callback=self.on_message)

    def setup_exchange_and_queue(self,queue,exchange):
        self._channel.queue_declare(queue=queue, durable=False)
        self._channel.exchange_declare(exchange=exchange, exchange_type='fanout', durable=True)
        self._channel.queue_bind(exchange=exchange, queue=queue,
                       routing_key='accounting.routing.key')
    
    def on_message(self, ch, method, properties, body):
        try:
            data = json.loads(body.decode('utf-8'))
            logger.info('Processing message: %s', data)
            asyncio.run(process_message(data))
            time.sleep(2)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error('Error processing message: %s', str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        while not self._closing:
            try:
                self.connect()
                logger.info('Start consuming')
                self._channel.start_consuming()
            except KeyboardInterrupt:
                self._closing = True
                logger.info('Stopping')
            except pika.exceptions.ConnectionClosedByBroker:
                self.reconnect()
            except pika.exceptions.AMQPConnectionError as err:
                logger.error(f"AMQP Connection Error: {err}")
                self.reconnect()
            except pika.exceptions.AMQPChannelError as err:
                logger.error(f"AMQP Channel Error: {err}")
                self.reconnect()
            except Exception as err:
                logger.error(f"Error: {err}")
                logger.warning('Consumer will retry connection in 5 seconds.')
                time.sleep(5)
            finally:
                if self._connection:
                    self._connection.close()    

    def reconnect(self):
        logger.warning('Connection closed. Reconnecting...')
        self._connection = None
        self._channel = None
        time.sleep(5)
        app.rabbitmq_connection()


    
    
