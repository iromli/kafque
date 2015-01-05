from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

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
