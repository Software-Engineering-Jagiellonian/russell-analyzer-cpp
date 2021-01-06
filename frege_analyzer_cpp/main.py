import json
import os
import sys

from frege_analyzer_cpp import config
from frege_analyzer_cpp.analyzer import CppAnalyzer
from frege_analyzer_cpp.database import Database
from frege_analyzer_cpp.database_connection_parameters import DatabaseConnectionParameters
from frege_analyzer_cpp.logger import logger
from frege_analyzer_cpp.rabbit import Rabbit


def process_received_message(message):
    try:
        return json.loads(message)['repo_id']
    except KeyError:
        logger.error('Received message does not contain repo_id node')
    except json.decoder.JSONDecodeError:
        logger.error('Received message is not a JSON')


def message_received_callback(channel, method, properties, body):
    channel.stop_consuming()
    message = body.decode('utf-8')
    logger.info(f'{config.QUEUE_IN} - Received:\n{message}')

    repo_id = process_received_message(message)
    if repo_id:
        logger.info(f'{config.QUEUE_IN} - Received repo_id={repo_id}')
        logger.info(f'Searching for file_paths for C++ (language_id = {config.LANGUAGE_ID}) with repo_id={repo_id}')
        file_paths = database.get_file_paths(repo_id)
        logger.info(f'Found {file_paths}')
        results = analyzer.analyze(file_paths)
        database.save_results(repo_id, results)
        rabbit.publish_message(repo_id)
    else:
        logger.error(f'{config.QUEUE_IN} - Skipping invalid message:\n{message}')

    channel.basic_ack(delivery_tag=method.delivery_tag)


def parse_environment(variable, optional=False, optional_value=None):
    try:
        return os.environ.get(variable, optional_value) if optional else os.environ[variable]
    except KeyError:
        logger.error(f'{variable} in environment var must be provided!')
        sys.exit(1)


if __name__ == '__main__':
    rabbitmq_host = parse_environment('RMQ_HOST')
    rabbitmq_port = int(parse_environment('RMQ_PORT', optional=True, optional_value='5672'))
    database_host = parse_environment('DB_HOST')
    database_name = parse_environment('DB_DATABASE')
    database_username = parse_environment('DB_USERNAME')
    database_password = parse_environment('DB_PASSWORD')

    database_parameters = DatabaseConnectionParameters(host=database_host, database=database_name,
                                                       username=database_username, password=database_password)

    analyzer = CppAnalyzer()
    database = Database(database_parameters)
    database.connect()
    rabbit = Rabbit(host=rabbitmq_host, port=rabbitmq_port)
    rabbit.consume_message(on_message_callback=message_received_callback)
