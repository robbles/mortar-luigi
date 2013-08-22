import abc
import datetime
import json

import luigi
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi.dynamodb import DynamoDBClient, HashKey, RangeKey, NUMBER, STRING
from mortar.luigi import mortartask

import logging
logger = logging.getLogger('luigi-interface')

def get_input_path(input_bucket, data_date, filename=None):
    path = 's3://%s/input/%s' % (input_bucket, data_date)
    if filename:
        path += ('/%s' % filename)
    return path

def get_output_path(output_bucket, data_date, filename=None):
    path = 's3://%s/output/%s' % (output_bucket, data_date)
    if filename:
        path += ('/%s' % filename)
    return path

def table_name(client_id, client_name, table_type, data_date):
    return '%s-%s-%s-%s' % (client_id, client_name, table_type, data_date)

def ii_table(client_id, client_name, data_date):
    return table_name(client_id, client_name, 'ii', data_date)

def ui_table(data_date):
    return table_name(client_id, client_name, 'ui', data_date)

def write_s3_token_file_out(out_file):
    with out_file.open('w') as token_file:
        token_file.write('%s' % datetime.datetime.utcnow().isoformat())

class RecsysException(Exception):
    pass

class RecsysTask(luigi.Task):
    """
    Generic recsys task.
    """

    # name of client (no spaces)
    client_name = luigi.Parameter()
    
    # client's ID
    client_id = luigi.Parameter()
   
    # bucket where data arrives
    input_bucket = luigi.Parameter()

    # bucket where we store intermediate and final results
    output_bucket = luigi.Parameter()

    # date of data to process
    data_date = luigi.DateParameter()

    def input_path(self, filename=None):
        return get_input_path(self.input_bucket, self.data_date, filename)

    def output_path(self, filename=None):
       return get_output_path(self.output_bucket, self.data_date, filename)

    def ii_table_name(self):
       return ii_table(self.client_id, self.client_name, self.data_date)

    def ui_table_name(self):
       return ui_table(self.client_id, self.client_name, self.data_date)
       
    def write_s3_token_file(self, out_file):
        write_s3_token_file_out(out_file)
    
    
class RecsysMortarProjectPigscriptTask(mortartask.MortarProjectPigscriptTask):
    """
    Task that runs a recsys pigscript on Mortar.
    """
    
    # name of client (no spaces)
    client_name = luigi.Parameter()
    
    # bucket where data arrives
    input_bucket = luigi.Parameter()
    
    # bucket where we store intermediate and final results
    output_bucket = luigi.Parameter()
    
    # date of data to process
    data_date = luigi.DateParameter()
    
    def input_path(self, filename=None):
        return get_input_path(self.input_bucket, self.data_date, filename)

    def output_path(self, filename=None):
       return get_output_path(self.output_bucket, self.data_date, filename)

    def ii_table_name(self):
       return ii_table(self.client_id, self.client_name, self.data_date)

    def ui_table_name(self):
       return ui_table(self.client_id, self.client_name, self.data_date)

    def write_s3_token_file(self, out_file):
       write_s3_token_file_out(out_file)

class CreateDynamoDBTable(RecsysTask):
    """
    Create new table in DynamoDB to serve recommendations.
    """
    read_throughput = luigi.IntParameter(1)
    write_throughput = luigi.IntParameter(1000)
    
    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table to create.
        """
        raise RuntimeError("Must provide a table_name to create")
        
    def output(self):
        path = '%s-%s' % (self.output_path(self.__class__.__name__), self.table_name())
        return [S3Target(path)]
    
    def run(self):
        dynamodb_client = DynamoDBClient()
        schema = [HashKey('from_id', data_type=STRING),
                  RangeKey('rank', data_type=NUMBER)]
        throughput={'read': self.read_throughput,
                    'write': self.write_throughput}
        table = dynamodb_client.create_table(self.table_name(), schema, throughput)

        # write token to note completion
        self.write_s3_token_file(self.output()[0])

class UpdateDynamoDBThroughput(RecsysTask):

    read_throughput =  luigi.IntParameter(50)
    write_throughput = luigi.IntParameter(1)

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table to update.
        """
        raise RuntimeError("Must provide a table_name to update")

    def output(self):
        path = '%s-%s' % (self.output_path(self.__class__.__name__), self.table_name())
        return [S3Target(path)]
    
    def run(self):
        dynamodb_client = DynamoDBClient()
        throughput={'read': self.read_throughput,
                    'write': self.write_throughput}
        table = dynamodb_client.update_throughput(self.table_name(), throughput)

        # write an output token to S3 to confirm that we finished
        self.write_s3_token_file(self.output()[0])

class SanityTestDynamoDBTable(RecsysTask):

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table to sanity test.
        """
        raise RuntimeError("Must provide a table_name to sanity test")
    
    def output(self):
        path = '%s-%s' % (self.output_path(self.__class__.__name__), self.table_name())
        return [S3Target(path)]

    def run(self):
        dynamodb_client = DynamoDBClient()
        table = dynamodb_client.get_table(self.table_name())

        # do a quick sanity check
        limit = 100
        results = [r for r in table.scan(limit=limit, to_id__null=False)]
        num_results = len(results)
        if num_results < limit:
            raise RecsysException('Sanity check failed: only found %s / %s expected results in table %s with a to_id field' % \
                (num_results, limit, table_name))

        # write an output token to S3 to confirm that we finished
        self.write_s3_token_file(self.output()[0])

class PromoteDynamoDBTablesToAPI(RecsysTask):
    
    # host for recsys API
    recsys_api_host = luigi.Parameter()
    
    @abc.abstractmethod
    def table_names(self):
        """
        Dictionary with table names to set.
        """
        raise RuntimeError("Must provide a dictionary of table names to set")
    
    def output(self):
        return [S3Target(self.output_path(self.__class__.__name__))]
    
    def run(self):
        self._set_tables()

        # write an output token to S3 to confirm that we finished
        self.write_s3_token_file(self.output()[0])
    
    def _client_update_endpoint(self):
        return '%s/v1/clients/%s' % (self.recsys_api_host, self.client_id)
    
    def _set_tables(self):
        tables = self.table_names()
        headers = {'Accept': 'application/json',
                   'Accept-Encoding': 'gzip',
                   'Content-Type': 'application/json',
                   'User-Agent': 'mortar-luigi'}
        url = self._client_update_endpoint()
        body = {'ii_table': self.table_names()['ii_table'],
                'ui_table': self.table_names()['ui_table']}
        auth = HTTPBasicAuth(configuration.get_config().get('recsys', 'email'),
                             configuration.get_config().get('recsys', 'password'))
        logger.info('Setting new tables to %s at %s' % (body, url))
        response = requests.put(url, data=json.dumps(body), auth=auth, headers=headers)
        response.raise_for_status()
