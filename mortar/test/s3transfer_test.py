import unittest, luigi, tempfile, os
from luigi import LocalTarget, configuration
from mock import patch
from mortar.luigi.s3transfer import LocalToS3Task
from luigi.s3 import S3Target, S3PathTask, S3Client
from moto import mock_s3
from luigi.mock import MockFile


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
    def test_output_path(self, mock_config):
        mock_config.get_config.return_value.get.return_value = "hello" 
        t = LocalToS3Task(s3_path, self.local_path, self.file_name) 
        luigi.build([t], local_scheduler=True)
        self.assertEquals(s3_path, t.input_file())
