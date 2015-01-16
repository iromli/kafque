from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import importlib
import logging
import time


def setup_logger(name, level):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s - %(name)s - "
                            "%(levelname)s - %(message)s")
    fmt.converter = time.gmtime
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def callback_from_string(name):
    module_name, attr = name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    callback = getattr(module, attr)
    return callback


def get_logging_level(name):
    return getattr(logging, name.upper(), "WARNING")
