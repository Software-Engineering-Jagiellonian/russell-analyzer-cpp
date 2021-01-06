import json
import sys
from time import sleep

import pika

from frege_analyzer_cpp import config
from frege_analyzer_cpp.logger import logger


class Rabbit:
    def __init__(self, host, port):
        logger.info(f"Connecting to RabbitMQ ({host}:{port})...")
        self.in_channel = self.create_channel(host, port, config.QUEUE_IN)
        self.out_channel = self.create_channel(host, port, config.QUEUE_OUT)

    @staticmethod
    def create_channel(host, port, queue):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
            channel = connection.channel()
            channel.confirm_delivery()
            channel.queue_declare(queue=queue, durable=True)
            return channel
        except pika.exceptions.AMQPConnectionError as exception:
            logger.error(f"AMQP Connection Error: {exception}")
        except KeyboardInterrupt:
            logger.info(" Exiting...")
            try:
                connection.close()
            except NameError:
                pass
            sys.exit(0)

    def consume_message(self, on_message_callback):
        while True:
            self.in_channel.basic_consume(queue=config.QUEUE_IN,
                                          auto_ack=False,
                                          on_message_callback=on_message_callback)

            logger.info(f'{config.QUEUE_IN} - Waiting for a message')
            self.in_channel.start_consuming()

    def publish_message(self, repo_id):
        while True:
            try:
                self.out_channel.basic_publish(
                    exchange='',
                    routing_key=config.QUEUE_OUT,
                    properties=pika.BasicProperties(delivery_mode=2),
                    body=self.prepare_body(repo_id)
                )
                break
            except pika.exceptions.NackError:
                logger.warning(f'{config.QUEUE_OUT} - Message rejected, sleeping for '
                               f'{config.PUBLISH_DELAY} seconds')
                sleep(config.PUBLISH_DELAY)

        logger.info(f'{config.QUEUE_OUT} - Message was received')

    @staticmethod
    def prepare_body(repo_id):
        return json.dumps({
            'repo_id': repo_id,
            'language_id': config.LANGUAGE_ID
        }).encode('utf-8')
