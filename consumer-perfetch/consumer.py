#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pika
from threading import Thread

msg = "TestTTL"

HOST = 'localhost'
USER = 'guest'
PASSWORD = 'guest'
VHOST = '/'

queue = 'prio_consumer'


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
        print("[x] {0} Received: {1}".format(method.consumer_tag, body))

    def consume(self, priority):
        self.channel.basic_consume(self.callback, queue=queue, consumer_tag="consumer_prio_{}".format(
            priority), no_ack=True, arguments={"x-priority": priority})

    def start(self):
        self.channel.start_consuming()

    def _queues_declare(self):
        self.channel.queue_declare(queue, durable=True, auto_delete=False)
        self.channel.queue_bind(exchange=self.exchange,
                                queue=queue, routing_key=queue)

    def send_msg(self, msg, queue):
        properties = pika.BasicProperties(delivery_mode=2)
        self.channel.basic_publish(
            exchange=self.exchange, routing_key=queue, body=msg, properties=properties)


def consumer():
    rmq_connection = RMQSender(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    for prio_num in range(3):
        rmq_connection.consume(prio_num)
    rmq_connection.start()


def producer():
    rmq_connection = RMQSender(
        host=HOST, user=USER, password=PASSWORD, virtual_host=VHOST)
    for _ in range(10):
        rmq_connection.send_msg("test", queue)

if __name__ == '__main__':
    Thread(target = consumer).start()
    Thread(target = producer).start()
    
