import unittest, luigi, tempfile, os
from luigi import LocalTarget, configuration
from mock import patch
from mortar.luigi.s3transfer import LocalToS3Task, S3ToLocalTask, S3TransferTask
from luigi.s3 import S3Target, S3PathTask, S3Client
import boto
from boto.s3.key import Key
from moto import mock_s3
import moto


s3_path = 's3://bucket/key'
AWS_ACCESS_KEY = "XXXXXX"
AWS_SECRET_KEY = "XXXXXX"


class TestS3ToLocalTask(unittest.TestCase):
    @mock_s3
    @patch("luigi.configuration")
    def setUp(self, mock_config):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFileContents = "I'm a temporary file for testing\nAnd this is the second line\nThis is the third."
        f.write(self.tempFileContents)
        f.close()
        self.tempFilePath = f.name
        self.file_name = f.name[f.name.rindex('/')+1:]
        self.local_path = f.name[:f.name.rindex('/')]
        self.s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = self.s3_client.s3.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key/%s' % self.file_name
        mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY 

    def tearDown(self):
        os.remove(self.tempFilePath)
    
    def test_path(self):
        """
        Tests that the input and output paths are set properly
        """
        t = S3ToLocalTask(s3_path, self.local_path, self.file_name) 
        t.client = self.s3_client
        luigi.build([t], local_scheduler=True)
        self.assertEquals(self.tempFilePath, t.output_target().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.input_target().path)
        self.assertEquals(self.tempFilePath, t.output()[0].path)

    def test_run_content(self):
        """
        Test that output at LocalTarget is same as input from S3 Target
        """
        t = S3ToLocalTask(s3_path, self.local_path, self.file_name) 
        t.client = self.s3_client 
        luigi.build([t], local_scheduler=True)
        target = t.output_target()
        self.assertEquals(target.open('r').read(), self.tempFileContents) 



"""
Test LocalToS3Task
"""
class TestLocalToS3Task(unittest.TestCase):
    @patch("luigi.configuration")
    def setUp(self, mock_config):
        
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        self.tempFileContents = "I'm a temporary file for testing\nAnd this is the second line\nThis is the third."
        self.tempFilePath = f.name
        self.file_name = f.name[f.name.rindex('/')+1:]
        self.local_path = f.name[:f.name.rindex('/')]

        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        self.mock = mock_s3()
        self.mock.start()
        self.s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY) # client needs to be set to T in order to mock it properly
        bucket = self.s3_client.s3.create_bucket('bucket')
        k = Key(bucket)
        k.key = 'key/%s' % self.file_name 
        k.set_contents_from_string(self.tempFileContents)

        mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY

    def tearDown(self):
        self.mock.stop()
        os.remove(self.tempFilePath)



    def test_path(self):
        """
        Tests that the input and output paths are set properly
        """
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        t.client = self.s3_client
        luigi.build([t], local_scheduler=True)
        self.assertEquals("%s/%s" % (self.local_path, self.file_name), t.input_target().path)
        self.assertEquals("%s/%s" % (s3_path, self.file_name), t.output()[0].path)
        self.assertEquals(t.output()[0].path, t.output_target().path)

    def test_run_content(self):
        """
        Test content at S3 target is same as local
        """
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        t.client = self.s3_client
        luigi.build([t], local_scheduler=True)
        self.assertTrue(t.output_target().exists())
        self.assertEquals(t.output_target().open('r').read(), self.tempFileContents)
