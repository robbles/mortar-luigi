
import unittest
from boto.dynamodb2.fields import HashKey, AllIndex, RangeKey
from boto.dynamodb2.types import STRING, NUMBER
import luigi
from luigi.s3 import S3Target, S3Client
from mock import patch
from mortar.luigi.dynamodb import DynamoDBClient, CreateDynamoDBTable, UpdateDynamoDBThroughput,\
    SanityTestDynamoDBTable, DynamoDBTaskException
from moto import mock_dynamodb2, mock_s3


AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

class TestCreateDynamoDBTableTask(CreateDynamoDBTable):
    read_throughput = 2
    write_throughput = 3
    hash_key = 'key1'
    hash_key_type = NUMBER
    range_key = 'range_key1'
    range_key_type = STRING
    indexes = [{'name': 'index1', 'range_key': 'range_key2', 'data_type': NUMBER}]

    def table_name(self):
        return 'dynamo_table1'

    def output_token(self):
        return S3Target('s3://mybucket/token_place')

    def requires(self):
        return None

class TestUpdateDynamoDBTableTask(UpdateDynamoDBThroughput):
    read_throughput = 8
    write_throughput = 16

    def table_name(self):
        return 'dynamo_table1'

    def output_token(self):
        return S3Target('s3://mybucket/token_place')

    def requires(self):
        return None

class TestSanityTestDynamoDBTableTask(SanityTestDynamoDBTable):
    hash_key = 'key1'
    hash_key_type = NUMBER

    min_total_results = 2
    non_null_fields = ['value']
    result_length = 1

    def ids(self):
        return [1, 2, 3, 4, 5]

    def table_name(self):
        return 'dynamo_table1'

    def output_token(self):
        return S3Target('s3://mybucket/token_place')

    def requires(self):
        return None

class TestDynamoDBClient(unittest.TestCase):

        @mock_dynamodb2
        def test_create_table(self):
            table_name = 'dynamo_table'
            schema = [HashKey('my_hash', data_type=STRING)]
            indexes = [AllIndex('IndexName', parts=[
                HashKey('my_hash', data_type=STRING),
                RangeKey('range_index', data_type=NUMBER)])]
            throughput = {'read': 2, 'write': 4}
            client = DynamoDBClient(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
            client.create_table(table_name, schema, throughput, indexes=indexes)

            table = client.get_table(table_name)
            self.assertEquals(2, table.throughput['read'])
            self.assertEquals(4, table.throughput['write'])
            self.assertEquals('my_hash', table.schema[0].name)
            # moto doesn't handle indexes in dynamodb2; can't test


        @mock_dynamodb2
        def test_update_throughput(self):
            table_name = 'dynamo_table'
            schema = [HashKey('my_hash', data_type=STRING)]
            indexes = [AllIndex('IndexName', parts=[
                HashKey('my_hash', data_type=STRING),
                RangeKey('range_index', data_type=NUMBER)])]
            throughput = {'read': 2, 'write': 4}
            client = DynamoDBClient(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
            client.create_table(table_name, schema, throughput, indexes=indexes)
            table = client.get_table(table_name)
            self.assertEquals(2, table.throughput['read'])
            self.assertEquals(4, table.throughput['write'])

            new_throughput = {'read': 8, 'write': 1}
            client.update_throughput(table_name, new_throughput)
            table = client.get_table(table_name)
            # verify new throughput
            self.assertEquals(8, table.throughput['read'])
            self.assertEquals(1, table.throughput['write'])

        @mock_dynamodb2
        @mock_s3
        @patch("luigi.configuration")
        def test_create_table_task(self, mock_config):
            mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY
            t = TestCreateDynamoDBTableTask()
            s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            s3_client.s3.create_bucket('mybucket')
            luigi.build([t], local_scheduler=True)

            client = DynamoDBClient(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

            table = client.get_table('dynamo_table1')
            self.assertEquals(2, table.throughput['read'])
            self.assertEquals(3, table.throughput['write'])
            self.assertEquals('key1', table.schema[0].name)

        @mock_dynamodb2
        @mock_s3
        @patch("luigi.configuration")
        def test_update_table_task(self, mock_config):
            mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY
            t = TestUpdateDynamoDBTableTask()

            # mock s3 location for writing output token
            s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            s3_client.s3.create_bucket('mybucket')

            # create table
            table_name = 'dynamo_table1'
            schema = [HashKey('my_hash', data_type=STRING)]
            indexes = [AllIndex('IndexName', parts=[
                HashKey('my_hash', data_type=STRING),
                RangeKey('range_index', data_type=NUMBER)])]
            throughput = {'read': 2, 'write': 4}
            client = DynamoDBClient(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
            client.create_table(table_name, schema, throughput, indexes=indexes)

            luigi.build([t], local_scheduler=True)

            table = client.get_table('dynamo_table1')
            self.assertEquals(8, table.throughput['read'])
            self.assertEquals(16, table.throughput['write'])

        @mock_dynamodb2
        @mock_s3
        @patch("luigi.configuration")
        def test_sanity_test_table_task(self, mock_config):
            mock_config.get_config.return_value.get.return_value = AWS_ACCESS_KEY
            t = TestSanityTestDynamoDBTableTask()

             # mock s3 location for writing output token
            s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            s3_client.s3.create_bucket('mybucket')

            # create table
            table_name = 'dynamo_table1'
            schema = [HashKey('my_hash', data_type=STRING)]
            indexes = [AllIndex('IndexName', parts=[
                HashKey('my_hash', data_type=STRING),
                RangeKey('range_index', data_type=NUMBER)])]
            throughput = {'read': 2, 'write': 4}
            client = DynamoDBClient(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
            client.create_table(table_name, schema, throughput, indexes=indexes)

            self.assertRaises(DynamoDBTaskException, luigi.build([t], local_scheduler=True))