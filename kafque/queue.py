from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging

from kafka import KafkaClient
from kafka.producer import SimpleProducer

from .helpers import setup_logger


class Queue(object):
    def __init__(self, topic, hosts=None, log_level=logging.WARNING):
        hosts = hosts or "localhost:9092"
        self.topic = "{}_{}".format("kafque", topic)
        self.client = KafkaClient(hosts)
        self.producer = SimpleProducer(self.client)
        self.logger = setup_logger(__name__, level=log_level)

    def enqueue(self, callback, *args, **kwargs):
        _callback = "{}.{}".format(callback.__module__, callback.__name__)

        job = json.dumps({
            "callback": _callback,
            "args": args,
            "kwargs": kwargs,
        })
        return self.producer.send_messages(str(self.topic), job)
