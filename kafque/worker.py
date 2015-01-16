from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
import signal

from kafka import KafkaClient
from kafka.consumer import SimpleConsumer

from .helpers import setup_logger
from .helpers import callback_from_string
from .queue import FailedQueue


class Worker(object):
    def __init__(self, topic, hosts=None, log_level=logging.WARNING):
        hosts = hosts or "localhost:9092"
        self.group = "kafque"
        self.topic = "{}_{}".format(self.group, topic)
        self.client = KafkaClient(hosts)
        self.client.ensure_topic_exists(str(self.topic))
        self.consumer = SimpleConsumer(
            self.client, str(self.group), str(self.topic), auto_commit=False)
        self.consumer.provide_partition_info()
        self.consumer.fetch_last_known_offsets()
        self.logger = setup_logger(__name__, level=log_level)

        self.failed_queue = None
        if self.topic != "{}_failed".format(self.group):
            self.failed_queue = FailedQueue(
                hosts=hosts, log_level=logging.ERROR)

    def handle_signals(self):
        def warm_shutdown(signum, frame):
            # TODO: if worker is busy, defer cleanup to cold_shutdown
            self.logger.debug("Got signal {}.".format(signum))
            self.logger.warning("Warm shut down.")
            raise SystemExit()

        signal.signal(signal.SIGINT, warm_shutdown)
        signal.signal(signal.SIGTERM, warm_shutdown)

    def run(self):
        self.logger.info("kafque worker started.")
        self.handle_signals()

        for partition, message in self.consumer:
            self.logger.debug("Offset {}".format(message.offset))
            job = json.loads(message.message.value)

            callback = callback_from_string(job.pop("callback"))
            try:
                result = callback(*job["args"], **job["kwargs"])
                self.logger.info(result)
                self.consumer.commit()
            except Exception as exc:
                self.logger.error(exc, exc_info=True)

                # TODO: set job as failed
                if self.failed_queue:
                    self.failed_queue.enqueue(
                        callback, args=job["args"], kwargs=job["kwargs"])
                    self.consumer.commit()
