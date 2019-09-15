import logger.sql_logger as sl
import logger.robot_logger as rl
import unittest
import os

class TestStateLogger(unittest.TestCase):


    def testInherit(self):
        rlogger = rl.RobotLogger()
        rlogger.add_topic('Age', 'int')
        rlogger.write('Age', 1234, "file.txt", True)
        rlogger.write('Age', "somee1", "file.txt", False)
        rlogger.write('Age', "somee", "file.txt", True)
        rlogger.write('Age',"Hello", "file.txt", True)


