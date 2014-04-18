import unittest
from mock import patch
from mortar.luigi.sqoop import MortarSqoopTask
import luigi
from moto import mock_s3

DATABASE_TYPE = 'some_dbtype'
DATABASE = 'mydatabase'
HOST = 'host'
PORT = '1234'
USERNAME = 'myusername'
PASSWORD = 'mypassword'
S3_PATH = 's3n://my_bucket'

class MortarSqoopTaskTest(MortarSqoopTask):
    def parameters(self):
        return {'dbtype' : DATABASE_TYPE,
                'database' : DATABASE,
                'host' : HOST,
                'port' : PORT, 
                'username' : USERNAME, 
                'password' : PASSWORD}

    def command(self):
        return 'test command'

    def output(self):
        return ''
    def arguments(self):
        return 'extra arguments'


EXPECTED_STR = 'mortar local:test command some_dbtype mydatabase extra arguments s3n://my_bucket -u myusername -p mypassword --host host:1234'

class TestMortarSqoopBase(unittest.TestCase):
    @mock_s3
    @patch("os.system")
    def test_run(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH)
        luigi.build([t], local_scheduler=True)
        self.assertEquals(t.command_str , EXPECTED_STR)

