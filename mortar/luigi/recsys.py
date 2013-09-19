import abc
import datetime
import json

import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi.dynamodb import DynamoDBClient, HashKey, RangeKey, NUMBER, STRING
from mortar.luigi import mortartask

import requests
from requests.auth import HTTPBasicAuth

import logging
logger = logging.getLogger('luigi-interface')

def get_input_path(input_bucket, data_date, filename=None, incremental=False):
    if incremental:
        path = 's3://%s/input' % input_bucket
    else:
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

def ui_table(client_id, client_name, data_date):
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

    # whether the data upload is incremental
    incremental = luigi.Parameter(False)

    def input_path(self, filename=None):
        return get_input_path(self.input_bucket, self.data_date, filename=filename, incremental=self.incremental)

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

    # whether the data upload is incremental
    incremental = luigi.Parameter(False)

    def input_path(self, filename=None):
        return get_input_path(self.input_bucket, self.data_date, filename=filename, incremental=self.incremental)

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

    rec_length = luigi.IntParameter(5)

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table to sanity test.
        """
        raise RuntimeError("Must provide a table_name to sanity test")
    
    def output(self):
        path = '%s-%s' % (self.output_path(self.__class__.__name__), self.table_name())
        return [S3Target(path)]

    @abc.abstractmethod
    def ids(self):
        return RuntimeError("Must provide list of ids to sanity test")

    def run(self):
        dynamodb_client = DynamoDBClient()
        table = dynamodb_client.get_table(self.table_name())

        # do a quick sanity check
        limit = 100
        results = [r for r in table.scan(limit=limit, to_id__null=False, score__null=False)]
        num_results = len(results)
        if num_results < limit:
            raise RecsysException('Sanity check failed: only found %s / %s expected results in table %s with a to_id & score field' % \
                (num_results, limit, table_name))

        # do a check on specific ids
        self._sanity_check_ids(table)

        # write an output token to S3 to confirm that we finished
        self.write_s3_token_file(self.output()[0])

    def _sanity_check_ids(self, table):
        failure_count = 0
        for from_id in self.ids():
            results = table.query(
                from_id__eq=from_id, limit=self.rec_length, reverse=True)
            if len(list(results)) < self.rec_length:
                failure_count += 1
                logger.info('Id %s only returned %s results.' % (from_id, len(list(results))))
        if failure_count > 2:
            raise RecsysException('Sanity check failed: %s ids in table %s failed to return sufficient results' % \
                (failure_count, table_name))


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


class VerifyApi(RecsysTask):

    rec_length = luigi.IntParameter(5)

    def output(self):
        return [S3Target(self.output_path(self.__class__.__name__))]

    def headers(self):
        return {'Accept': 'application/json',
                   'Accept-Encoding': 'gzip',
                   'Content-Type': 'application/json',
                   'User-Agent': 'mortar-luigi'}

    def auth(self):
        return HTTPBasicAuth(configuration.get_config().get('recsys', 'email'),
                             configuration.get_config().get('recsys', 'password'))

    def run(self):
        self._verify_api()

        # write an output token to S3 to confirm that we finished
        self.write_s3_token_file(self.output()[0])

    @abc.abstractmethod
    def _verify_api(self):
        raise RuntimeError("Must implement _verify_api!")

    def _verify_endpoint(self, endpoint_func, ids):
        num_empty = 0
        for item_id in ids:
            endpoint = endpoint_func(item_id)
            response = requests.get(endpoint, auth=self.auth(), headers=self.headers())
            response.raise_for_status()
            if len(response.json()['recommended_items']) < self.rec_length:
                num_empty += 1
                logger.info('Id %s only returned %s results.' % (item_id, len(response.json()['recommended_items'])))
        if num_empty > 2:
            raise RecsysException('API verification failed: %s items had insufficient endpoint results' % num_empty)


class VerifyItemItemApi(VerifyApi):

    # host for recsys API
    recsys_api_host = luigi.Parameter()

    @abc.abstractmethod
    def item_ids(self):
        return RuntimeError("Must provide list of ids to sanity verify")

    def _item_endpoint(self, item_id):
        return '%s/v1/recommend/items/%s' % (self.recsys_api_host, item_id)

    def _multisources_endpoint(self, item_id):
        return '%s/v1/recommend/multisources?item_id=%s' % (self.recsys_api_host, item_id)

    def _verify_api(self):
        self._verify_endpoint(self._item_endpoint, self.item_ids())
        self._verify_endpoint(self._multisources_endpoint, self.item_ids())

class VerifyUserItemApi(VerifyApi):

    # host for recsys API
    recsys_api_host = luigi.Parameter()

    @abc.abstractmethod
    def user_ids(self):
        return RuntimeError("Must provide list of ids to sanity verify")

    def _user_endpoint(self, user_id):
        return '%s/v1/recommend/users/%s' % (self.recsys_api_host, user_id)

    def _verify_api(self):
         self._verify_endpoint(self._user_endpoint, self.user_ids())