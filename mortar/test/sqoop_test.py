import unittest
import os
from mock import patch
from mortar.luigi.sqoop import MortarSqoopTask
import mortar.luigi.sqoop as mortar_sqoop
import luigi
from moto import mock_s3

DATABASE_TYPE = 'some_dbtype'
DATABASE = 'mydatabase'
HOST = 'host'
PORT = '1234'
USERNAME = 'myusername'
PASSWORD = 'mypassword'
S3_PATH = 's3n://my_bucket'
AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

class MortarSqoopTaskTest(MortarSqoopTask):
    def parameters(self):
        return {'dbtype' : DATABASE_TYPE,
                'database' : DATABASE,
                'host' : HOST,
                'port' : PORT, 
                'username' : USERNAME, 
                'password' : PASSWORD}
    def aws_params(self):
        return {'aws_access_key_id' : AWS_ACCESS_KEY,
                'aws_secret_access_key' : AWS_SECRET_KEY}

    def command(self):
        return 'test_command'

    def output(self):
        return ''
    def arguments(self):
        return ['extra arguments',]



EXPECTED_ARGV = ['mortar', 'local:test_command', 'some_dbtype', 'mydatabase', 'extra arguments', 's3n://my_bucket', '-u', 'myusername', '-p', 'mypassword', '--host', 'host:1234',]

class TestMortarSqoopBase(unittest.TestCase):
    @patch.object(mortar_sqoop, 'check_output')
    def test_run(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH)
        luigi.build([t], local_scheduler=True)
        self.assertEquals(EXPECTED_ARGV, t.argv)
        self.assertEquals(os.environ['AWS_ACCESS_KEY'], AWS_ACCESS_KEY)
        self.assertEquals(os.environ['AWS_SECRET_KEY'], AWS_SECRET_KEY)

    @patch.object(mortar_sqoop, 'check_output')
    def test_run_options_with_driver_jar(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH, driver_jar='some/path')
        luigi.build([t], local_scheduler=True)
        option_string = EXPECTED_ARGV + ['-r', 'some/path']
        self.assertEqual(option_string, t.argv)

    @patch.object(mortar_sqoop, 'check_output')
    def test_run_options_with_jdbc_jar(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH, jdbc_driver='some/path')
        luigi.build([t], local_scheduler=True)
        option_string = EXPECTED_ARGV + ['-j', 'some/path']
        self.assertEqual(option_string, t.argv)

    @patch.object(mortar_sqoop, 'check_output')
    def test_run_options_with_direct(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH, direct=True)
        luigi.build([t], local_scheduler=True)
        option_string = EXPECTED_ARGV + ['--direct']
        self.assertEqual(option_string, t.argv)

    @patch.object(mortar_sqoop, 'check_output')
    def test_run_options_with_all_direct(self, os_mock):
        t = MortarSqoopTaskTest(path=S3_PATH, direct=True,
                                jdbc_driver='jdbc/path', driver_jar='jar/path')
        luigi.build([t], local_scheduler=True)
        option_string = EXPECTED_ARGV + ['-r', 'jar/path', '--direct', '-j', 'jdbc/path']
        self.assertEqual(option_string, t.argv)

