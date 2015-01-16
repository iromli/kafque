from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import click

from .worker import Worker
from .helpers import get_logging_level


@click.command()
@click.argument("topic")
@click.option("--hosts", "-H", default="",
              help="Comma-separated kafka servers.")
@click.option("--loglevel", "-L", default="WARNING", help="Logging level.")
def run_worker(topic, hosts, loglevel):
    worker = Worker(topic, hosts, log_level=get_logging_level(loglevel))
    worker.run()
