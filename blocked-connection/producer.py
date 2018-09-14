#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pika
from threading import Thread
from time import sleep

HOST = 'localhost'
USER = 'guest'
PASSWORD = 'guest'
VHOST = '/'

queue = 'blocking_queue'


class RMQSender():
    def __init__(self, exchange='messages', host='localhost', user='guest', password='guest', virtual_host='/'):
        self.exchange = exchange
        self.virtual_host = virtual_host
        self.credentials = pika.PlainCredentials(
            user, password)
        self.parameters = pika.ConnectionParameters(
            host=host, virtual_host=virtual_host, credentials=self.credentials)
        self.rmq_connect()

    def close(self):
        self.connection.close()

    def rmq_connect(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange, exchange_type='direct', durable=True, auto_delete=False)
        self._queues_declare()

    def callback(self, ch, method, properties, body):
        print("[x] Received: {0}".format(body))

    def consume(self, queue):
        self.channel.basic_consume(self.callback, queue=queue, no_ack=True)

    def start(self):
        self.channel.start_consuming()

    def _queues_declare(self):
        self.channel.queue_declare(
            queue, durable=True, auto_delete=False)
        self.channel.queue_bind(exchange=self.exchange,
                                queue=queue, routing_key=queue)

    def block(self):
        pass
    
    def unblock(self):
        pass

    def blocked_connection(self):
        self.channel.connection.add_on_connection_blocked_callback(self.block)
    
    def unblocked_connection(self):
        self.channel.connection.add_on_connection_blocked_callback(self.unblock)

    def send_msg(self, msg, queue):
        properties = pika.BasicProperties(delivery_mode=2)
        self.channel.basic_publish(
            exchange=self.exchange, routing_key=queue, body=msg, properties=properties)


def consumer():
    rmq_connection = RMQSender(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    rmq_connection.consume(queue)
    rmq_connection.start()


def producer():
    rmq_connection = RMQSender(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    rmq_connection.block()
    rmq_connection.unblock()
    for i in range(6):
        if i == 3:
            rmq_connection.block()
        rmq_connection.send_msg("test{}".format(i), queue)


if __name__ == '__main__':
    Thread(target=producer).start()
    sleep(10)
    Thread(target=consumer).start()
