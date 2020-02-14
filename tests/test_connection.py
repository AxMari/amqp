"""Connects to the RabbitMQ server at RABBITMQ_HOST:RABBITMQ_PORT,
creates a test queue, adds a message to it, then pulls it off and
disconnects.
"""
import pika
import os
import sys
import secrets

REQUIRED_ENV_VARS = [
    'AMQP_HOST', 'AMQP_PORT', 'AMQP_USERNAME',
    'AMQP_PASSWORD', 'AMQP_VHOST'
]


def main():
    for evar in REQUIRED_ENV_VARS:
        if not os.environ.get(evar):
            print(f'Missing required env var {evar}')
            sys.exit(1)

    print('Connecting to AMQP broker..')
    parameters = pika.ConnectionParameters(
        pika.ConnectionParameters(os.environ['AMQP_HOST']),
        int(os.environ['AMQP_PORT']),
        os.environ['AMQP_VHOST'],
        pika.PlainCredentials(
            os.environ['AMQP_USERNAME'], os.environ['AMQP_PASSWORD']
        )
    )
    connection = pika.BlockingConnection([parameters])

    print('Opening channel..')
    channel = connection.channel()
    print('Declaring queue..')
    channel.queue_declare(queue='hello')
    print('Basic publish..')
    pub_body = secrets.token_urlsafe(16)
    channel.basic_publish(
        exchange='', routing_key='hello', body=pub_body)
    print('Closing connection..')
    connection.close()
    channel = None

    print('Reconnecting..')
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['AMQP_HOST']))
    print('Opening channel..')
    channel = connection.channel()
    print('Performing basic get..')
    con_body = None
    for method_frame, properties, body in channel.consume('hello'):
        channel.basic_ack(method_frame.delivery_tag)
        con_body = body
        break
    print('Cancelling channel..')
    channel.close()
    print('Closing connection..')
    connection.close()

    if con_body != pub_body:
        print(f'Consumed {con_body} but sent {pub_body} (they do not match!)')
        sys.exit(1)


if __name__ == '__main__':
    main()
