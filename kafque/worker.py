from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import importlib
import json
import logging
import signal
import sys
import traceback

from kafka import KafkaClient
from kafka.consumer import SimpleConsumer

from .helpers import setup_logger


class Worker(object):
    def __init__(self, group, topic, hosts=None, log_level=logging.WARNING):
        hosts = hosts or "localhost:9092"
        self.group = "{}_{}".format("kafque", group)
        self.topic = "{}_{}".format("kafque", topic)
        self.client = KafkaClient(hosts)
        self.consumer = SimpleConsumer(
            self.client, str(self.group), str(self.topic))
        self.logger = setup_logger(__name__, level=log_level)

    def dequeue(self, entry):
        msg = entry.message.value
        return json.loads(msg)

    def perform(self, job):
        module_name, attr = job["callback"].rsplit(".", 1)
        module = importlib.import_module(module_name)
        callback = getattr(module, attr)
        return callback(*job["args"], **job["kwargs"])

    def get_exc_string(self):
        exc_info = sys.exc_info()
        exc_string = "".join(traceback.format_exception(*exc_info))
        return exc_string

    def handle_signals(self):
        def warm_shutdown(signum, frame):
            self.logger.debug("Got signal {}".format(signum))
            self.logger.warning("Warm shut down.")
            raise SystemExit()

        signal.signal(signal.SIGINT, warm_shutdown)
        signal.signal(signal.SIGTERM, warm_shutdown)

    def run(self):
        self.logger.info("kafque worker started.")
        self.handle_signals()

        for message in self.consumer:
            job = self.dequeue(message)
            try:
                result = self.perform(job)
                self.logger.info(result)
            except Exception:
                # TODO: set job as failed
                exc_string = self.get_exc_string()
                self.logger.error(exc_string)
