import unittest
import luigi
from luigi.s3 import S3Target, S3Client

from mock import Mock
from mock import patch

from mortar.luigi.mongodb import SanityTestMongoDBCollection
from mortar.luigi.mongodb import MongoDBTaskException
from moto import mock_s3

AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

class TestSanityTestMongoDBCollectionTask(SanityTestMongoDBCollection):
    id_field = "_id"

    def ids(self):
        return [1,2,3,4,5]

    def collection_name(self):
        return "col"

    def output_token(self):
        return S3Target('s3://mybucket/mongo_token')

    def requires(self):
        return None

class TestMongoDB(unittest.TestCase):

    @mock_s3
    @patch("luigi.configuration")
    @patch("pymongo.collection.Collection")
    @patch("mortar.luigi.mongodb.target_factory")
    def test_sanity_test_task__fails_when_sentinal_ids_not_found(self, mock_tf, mock_col, mock_config):
        mock_config.get_config.return_value.get.return_value = "Mocky"

        t = TestSanityTestMongoDBCollectionTask()
        t.min_total_results = 0
        t._get_collection = Mock()
        t._get_collection.return_value = mock_col
        self._set_mock_mongo_count_query(mock_col, 0)

        # mock s3 location for writing output token
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        luigi.build([t], local_scheduler=True)
        self.assertFalse(mock_tf.write_file.called)

    @mock_s3
    @patch("luigi.configuration")
    @patch("pymongo.collection.Collection")
    @patch("mortar.luigi.mongodb.target_factory")
    def test_sanity_test_task__passes(self, mock_tf, mock_col, mock_config):
        mock_config.get_config.return_value.get.return_value = "Mocky"

        t = TestSanityTestMongoDBCollectionTask()
        t._get_collection = Mock()
        t._get_collection.return_value = mock_col
        self._set_mock_mongo_count_query(mock_col, t.result_length)

        # mock s3 location for writing output token
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        luigi.build([t], local_scheduler=True)
        self.assertTrue(mock_tf.write_file.called)

    def _set_mock_mongo_count_query(self, mock_col, count):
        mock_col.find.return_value = mock_col
        mock_col.limit.return_value = mock_col
        mock_col.count.return_value = count

