import os
from unittest import * # noqa
from unittest import TestCase as OriginalTestCase
import logging


class Unittest(OriginalTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        msg = "grep_pater_8u2w: {}".format(os.environ.get('PYTHON_PATH'))
        print(msg)
        logging.info(msg)
