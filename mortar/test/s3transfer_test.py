import unittest, luigi, tempfile, os
from luigi import LocalTarget, configuration
from mock import patch
from mortar.luigi.s3transfer import LocalToS3Task, S3ToLocalTask
from luigi.s3 import S3Target, S3PathTask, S3Client
from moto import mock_s3
from luigi.mock import MockFile
import boto
from boto.s3.key import Key


s3_path = 's3://bucket/key'

class TestS3ToLocalTask(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        self.tempFileContents = "I'm a temporary file for testing\nAnd this is the second line\nThis is the third."
        f.write(self.tempFileContents)
        f.close()
        self.tempFilePath = f.name
        self.file_name = f.name[f.name.rindex('/')+1:]
        self.local_path = f.name[:f.name.rindex('/')]

    def tearDown(self):
        os.remove(self.tempFilePath)

    @mock_s3
    @patch("luigi.configuration")
    def test_path(self, mock_config):
        conn = boto.connect_s3()
          # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        bucket = conn.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key' 
        mock_config.get_config.return_value.get.return_value = "hello" 
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.tempFilePath, t.input_file().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.output_file().path)
        self.assertEquals("%s/LocalToS3Task" % self.local_path, t.output()[0].path)


class TestLocalToS3Task(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        self.tempFileContents = "I'm a temporary file for testing\nAnd this is the second line\nThis is the third."
        f.write(self.tempFileContents)
        f.close()
        self.tempFilePath = f.name
        self.file_name = f.name[f.name.rindex('/')+1:]
        self.local_path = f.name[:f.name.rindex('/')]

    def tearDown(self):
        os.remove(self.tempFilePath)


    @mock_s3
    @patch("luigi.configuration")
    def test_path(self, mock_config):
        conn = boto.connect_s3()
          # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        bucket = conn.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key' 
        mock_config.get_config.return_value.get.return_value = "hello" 
        t = S3ToLocalTask(s3_path, self.local_path, self.file_name) 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.tempFilePath, t.output_file().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.input_file().path)
        self.assertEquals("%s/S3ToLocalTask" % self.local_path, t.output()[0].path)





