# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import abc
import time

import boto.dynamodb2
from boto.dynamodb2.exceptions import DynamoDBError
from boto.dynamodb2.fields import HashKey, RangeKey, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING

import luigi
import logging
from mortar.luigi import target_factory

logger = logging.getLogger('luigi-interface')

class DynamoDBClient(object):
    """
    A limited client for interacting with DynamoDB.
    """

    # polling time and timeout for table creation and ramp-up
    TABLE_OPERATION_RESULTS_POLLING_SECONDS = 5.0
    TABLE_OPERATION_RESULTS_TIMEOUT_SECONDS = 60.0 * 30.0

    def __init__(self, region='us-east-1', aws_access_key_id=None, aws_secret_access_key=None):
        if not aws_access_key_id:
            aws_access_key_id = luigi.configuration.get_config().get('dynamodb', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = luigi.configuration.get_config().get('dynamodb', 'aws_secret_access_key')
        self.dynamo_cx = boto.dynamodb2.connect_to_region(
             region,
             aws_access_key_id=aws_access_key_id,
             aws_secret_access_key=aws_secret_access_key,
             is_secure=True)

    def create_table(self, table_name, schema, throughput, indexes=None):
        """
        Create a new dynamoDB table and block until it is ready to use.
        """
        table = Table.create(table_name,
            schema=schema,
            throughput=throughput,
            connection=self.dynamo_cx,
            indexes=indexes
        )
        logger.info('Created new dynamodb table %s with schema %s' % \
            (table_name, schema))
        return self._poll_until_table_active(table)
    
    def get_table(self, table_name):
        """
        Fetch a table from DynamoDB.
        
        NOTE: this is a somewhat expensive operation, 
              which must query dynamo for the current state of the table
        """
        table = Table(table_name, connection=self.dynamo_cx)
    
        # must describe the table, or it doesn't have the correct throughput values
        table.describe()
    
        return table

    def update_throughput(self, table_name, throughput):
        """
        Update a table's throughput in the stepwise fashion required for DynamoDB, 
        polling until complete.
        """
        table = self.get_table(table_name)

        # can only go up by 2X at a time; can go as far down in one time as wanted
        i = 0
        while (table.throughput['read'] != throughput['read']) or \
              (table.throughput['write'] != throughput['write']):
            request_throughput = {'read': min(throughput['read'], 2 * table.throughput['read']),
                                  'write': min(throughput['write'], 2 * table.throughput['write'])}
            logger.info('Round %s: Updating table to throughput %s' % (i, request_throughput))
            table.update(request_throughput)
            table = self._poll_until_table_active(table)
            i += 1

        return table

    def _poll_until_table_active(self, table):
        start_time = time.time()
        is_table_ready = False
        while (not is_table_ready) and (time.time() - start_time < DynamoDBClient.TABLE_OPERATION_RESULTS_TIMEOUT_SECONDS):
            try:
                describe_result = table.describe()
                status = describe_result['Table']['TableStatus']
                if status == 'ACTIVE':
                    logger.info('Table %s is ACTIVE with throughput %s' % (table.table_name, table.throughput))
                    is_table_ready = True
                else:
                    logger.debug('Table %s is in status %s' % (table.table_name, status))
                    time.sleep(DynamoDBClient.TABLE_OPERATION_RESULTS_POLLING_SECONDS)
            except DynamoDBError, e:
                logger.error('Error querying DynamoDB for table status; retrying. Error: %s' % e)

        if not is_table_ready:
            raise RuntimeError('Timed out waiting for DynamoDB table %s to be ACTIVE' % table.table_name)

        return table

class DynamoDBTask(luigi.Task):
    """
    Class for tasks interacting with DynamoDB.
    """

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table.
        """
        raise RuntimeError("Must provide a table_name")

    @abc.abstractmethod
    def output_token(self):
        """
        Token to be written out on completion of the task.
        """
        raise RuntimeError("Must provide an output token")

    def output(self):
        return self.output_token()


class CreateDynamoDBTable(DynamoDBTask):
    """
    Create new table in DynamoDB to serve recommendations.
    This task will fail if the requested table name already exists.
    Table creation in DynamoDB takes between several seconds and several minutes; this task will
        block until creation has finished.
    """

    # Initial read throughput of created table
    read_throughput = luigi.IntParameter()

    # Initial write throughput of created table
    write_throughput = luigi.IntParameter()

    # Name of the primary hash key for this table
    hash_key = luigi.Parameter()

    # Type of the primary hash key (boto.dynamodb2.types)
    hash_key_type = luigi.Parameter()

    # Name of the primary range key for this table, if it exists
    range_key = luigi.Parameter()

    # Type of the primary range key for this table, if it exists (boto.dynamodb2.types)
    range_key_type = luigi.Parameter()

    # Secondary indexes of the table, provided as a list of dictionaries
    # [ {'name': sec_index, 'range_key': range_key_name, 'data_type': NUMBER} ]
    indexes = luigi.Parameter(None)

    def generate_indexes(self):
        """
        Create boto-friendly index data structure.
        """
        all_index = []
        for index in self.indexes:
            all_index.append(AllIndex(index['name'], parts=[
                HashKey(self.hash_key, data_type=self.range_key_type),
                RangeKey(index['range_key'], data_type=index['data_type'])]))
        return all_index

    def run(self):
        """
        Create the DynamoDB table.
        """
        dynamodb_client = DynamoDBClient()
        schema = [HashKey(self.hash_key, data_type=self.hash_key_type)]
        if self.range_key:
            schema.append(RangeKey(self.range_key, data_type=self.range_key_type))
        throughput={'read': self.read_throughput,
                    'write': self.write_throughput}
        if self.indexes:
            dynamodb_client.create_table(self.table_name(), schema, throughput, indexes=self.generate_indexes())
        else:
            dynamodb_client.create_table(self.table_name(), schema, throughput)

        # write token to note completion
        target_factory.write_file(self.output_token())


class UpdateDynamoDBThroughput(DynamoDBTask):
    """
    Update the throughput of an existing DynamoDB table.
    Task will fail if table_name does not exist.
    """

    # Target read throughput
    read_throughput =  luigi.IntParameter()

    # Target write throughput
    write_throughput = luigi.IntParameter()

    def run(self):
        """
        Update DynamoDB table throughput.
        """
        dynamodb_client = DynamoDBClient()
        throughput={'read': self.read_throughput,
                    'write': self.write_throughput}
        dynamodb_client.update_throughput(self.table_name(), throughput)

        # write token to note completion
        target_factory.write_file(self.output_token())


class SanityTestDynamoDBTable(DynamoDBTask):
    """
    General check that the contents of a DynamoDB table exist and contain sentinel ids.
    """

    # Name of the primary hash key for this table
    hash_key = luigi.Parameter()

    # number of entries required to be in the table
    min_total_results = luigi.IntParameter(100)

    # when testing total entries, require that these field names not be null
    non_null_fields = luigi.Parameter([])

    # number of results required to be returned for each primary key
    result_length = luigi.IntParameter(5)

    # when testing specific ids, how many are allowed to fail
    failure_threshold = luigi.IntParameter(2)


    @abc.abstractmethod
    def ids(self):
        """
        Sentinel ids to check
        """
        return RuntimeError("Must provide list of ids to sanity test")

    def run(self):
        """
        Run sanity check.
        """
        dynamodb_client = DynamoDBClient()
        table = dynamodb_client.get_table(self.table_name())

        # check that the table contains at least min_total_results entries
        limit = self.min_total_results
        kw = {'limit': limit}
        for field in self.non_null_fields:
            kw['%s__null' % field] = False
        results = [r for r in table.scan(**kw)]
        num_results = len(results)
        if num_results < limit:
            exception_string = 'Sanity check failed: only found %s / %s expected results in table %s with a to_id & score field' % \
                    (num_results, limit, self.table_name())
            logger.warn(exception_string)
            raise DynamoDBTaskException(exception_string)

        # do a check on specific ids
        self._sanity_check_ids(table)

        # write token to note completion
        target_factory.write_file(self.output_token())

    def _sanity_check_ids(self, table):
        failure_count = 0
        kw = {'limit': self.result_length}
        for id in self.ids():
            kw['%s__eq' % self.hash_key] = id
            results = table.query(**kw)
            if len(list(results)) < self.result_length:
                failure_count += 1
                logger.info('Id %s only returned %s results.' % (id, len(list(results))))
        if failure_count > self.failure_threshold:
            exception_string = 'Sanity check failed: %s ids in table %s failed to return sufficient results' % \
                    (failure_count, self.table_name())
            logger.warn(exception_string)
            raise DynamoDBTaskException(exception_string)

class DynamoDBTaskException(Exception):
    """
    Exception thrown by DynamoDBTasks
    """
    pass
