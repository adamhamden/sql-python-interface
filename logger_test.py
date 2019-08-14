import logger.sql_logger as sl
import logger.robot_logger as rl
import unittest
import os

class TestStateLogger(unittest.TestCase):


    def testInherit(self):
        rlogger = sl.SQLLogger()
        rlogger.add_topic('Age', 'int')
        rlogger.write('Age', 1234, True)
        rlogger.backup()
        rlogger.write('Age', 5678, True)
        rlogger.backup()
        rlogger.write('Age',9910013, True)


