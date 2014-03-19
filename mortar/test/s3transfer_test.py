import unittest, luigi, tempfile, os
from luigi import LocalTarget, configuration
from mock import patch
from mortar.luigi.s3transfer import LocalToS3Task, S3ToLocalTask
from luigi.s3 import S3Target, S3PathTask, S3Client
from luigi.mock import MockFile
import boto
from boto.s3.key import Key
from moto import mock_s3
import moto


s3_path = 's3://bucket/key'
AWS_ACCESS_KEY = "XXXXXX"
AWS_SECRET_KEY = "XXXXXX"

class TestS3ToLocalTask(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
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
        conn = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = conn.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key' 
        mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY 
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.tempFilePath, t.input_target().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.output_target().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.output()[0].path)

    @mock_s3
    @patch("luigi.configuration")
    def test_run_content(self, mock_config):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = s3_client.s3.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key/%s' % self.file_name
        #mock_config.get_config.return_value.get.return_value = "hello" 
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        t.client = s3_client 
        luigi.build([t], local_scheduler=True)
        s3_target = S3Target(t.output_target().path, client=s3_client)
        #self.assertEquals(s3_target.open('r').read(), self.tempFileContents) 

class TestLocalToS3Task(unittest.TestCase):

    @mock_s3
    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        self.tempFileContents = "I'm a temporary file for testing\nAnd this is the second line\nThis is the third."
        self.tempFilePath = f.name
        self.file_name = f.name[f.name.rindex('/')+1:]
        self.local_path = f.name[:f.name.rindex('/')]
        self.s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        bucket = self.s3_client.s3.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key/%s' % self.file_name 
        k.set_contents_from_string(self.tempFileContents)

    def tearDown(self):
        os.remove(self.tempFilePath)


    @mock_s3
    @patch("luigi.configuration")
    def test_path(self, mock_config):
        mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY
        t = S3ToLocalTask(s3_path, self.local_path, self.file_name) 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.tempFilePath, t.output_target().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.input_target().path)
        self.assertEquals("%s/%s" % (self.local_path, self.file_name), t.output()[0].path)

    @mock_s3
    @patch("luigi.configuration")
    def test_run_content(self, mock_config):
        mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY
        t = S3ToLocalTask(s3_path, self.local_path, self.file_name) 
        self.t = t
        t.client = self.s3_client
        luigi.build([t], local_scheduler=True)
        self.assertTrue(t.output_target().exists())
        #self.assertEquals(t.output_target().open('r').read(), self.tempFileContents)
          
        




