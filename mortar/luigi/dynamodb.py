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
    A boto-based client for interacting with DynamoDB from Luigi.

    seealso:: https://help.mortardata.com/technologies/luigi/dynamodb_tasks
    """

    # interval to wait between polls to DynamoDB API in seconds
    TABLE_OPERATION_RESULTS_POLLING_SECONDS = 5.0

    # timeout for DynamoDB table creation and ramp-up in seconds
    TABLE_OPERATION_RESULTS_TIMEOUT_SECONDS = 60.0 * 30.0

    def __init__(self, region='us-east-1', aws_access_key_id=None, aws_secret_access_key=None):
        """

        :type region: str
        :param region: AWS region where your DynamoDB instance is located. Default: us-east-1.

        :type aws_access_key_id: str
        :param aws_access_key_id: AWS Access Key ID. If not provided, will be looked up from luigi configuration in dynamodb.aws_access_key_id.

        :type aws_secret_access_key: str
        :param aws_secret_access_key: AWS Secret Access Key. If not provided, will be looked up from luigi configuration in dynamodb.aws_secret_access_key.
        """
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
        Create a new DynamoDB table and block until it is ready to use.

        :type table_name: str
        :param table_name: Name for table

        :type schema: list of boto.dynamodb2.fields.HashKey
        :param schema: Table schema

        :type throughput: dict with {'read': read_throughput, 'write': write_throughput}
        :param throughput: Initial table throughput

        :type indexes: list of boto.dynamodb2.fields.AllIndex
        :param indexes: Initial indexes for the table. Default: no indexes.

        :rtype: boto.dynamodb2.table.Table:
        :returns: Newly created Table
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
        Fetch a Table from DynamoDB.

        NOTE: this is a somewhat expensive operation,
        which must query dynamo for the current state
        of the table.

        :type table_name: str
        :param table_name: Name of Table to load

        :rtype: boto.dynamodb2.table.Table:
        :returns: Requested Table
        """
        table = Table(table_name, connection=self.dynamo_cx)

        # must describe the table, or it doesn't have the correct throughput values
        table.describe()

        return table

    def update_throughput(self, table_name, throughput):
        """
        Update a table's throughput, using the stepwise
        fashion of increasing throughput by 2X each iteration,
        until the table has reached desired throughput.

        note:: As of Oct 2014, stepwise update is no longer required for DynamoDB.

        :rtype: boto.dynamodb2.table.Table:
        :returns: Table with updated throughput
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
    Superclass for Luigi Tasks interacting with DynamoDB.

    seealso:: https://help.mortardata.com/technologies/luigi/dynamodb_tasks
    """

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table on which operation should be performed.

        :rtype: str:
        :returns: table_name for operation
        """
        raise RuntimeError("Please implement the table_name method")

    @abc.abstractmethod
    def output_token(self):
        """
        Luigi Target providing path to a token that indicates
        completion of this Task.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        raise RuntimeError("Please implement the output_token method")

    def output(self):
        """
        The output for this Task. Returns the output token
        by default, so the task only runs if the token does not 
        already exist.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        return self.output_token()


class CreateDynamoDBTable(DynamoDBTask):
    """
    Luigi Task to create a new table in DynamoDB.

    This Task writes an output token to the location designated
    by the `output_token` method to indicate that the
    table has been successfully create. The Task will fail 
    if the requested table name already exists.

    Table creation in DynamoDB takes between several seconds and several minutes; this Task will
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
    range_key = luigi.Parameter(None)

    # Type of the primary range key for this table, if it exists (boto.dynamodb2.types)
    range_key_type = luigi.Parameter(None)

    # Secondary indexes of the table, provided as a list of dictionaries
    # [ {'name': sec_index, 'range_key': range_key_name, 'data_type': NUMBER} ]
    indexes = luigi.Parameter(None)

    def _generate_indexes(self):
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
            dynamodb_client.create_table(self.table_name(), schema, throughput, indexes=self._generate_indexes())
        else:
            dynamodb_client.create_table(self.table_name(), schema, throughput)

        # write token to note completion
        target_factory.write_file(self.output_token())


class UpdateDynamoDBThroughput(DynamoDBTask):
    """
    Luigi Task to update the throughput of an existing DynamoDB table.

    This Task writes an output token to the location designated
    by the `output_token` method to indicate that the
    table has been successfully updated. This Task will fail if the 
    table does not exist.
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
    Luigi Task to sanity check that that a set of sentinal IDs
    exist in a DynamoDB table (usually after loading it with data).

    This Task writes an output token to the location designated
    by the `output_token` method to indicate that the
    Task has been successfully completed.
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
        List of sentinal IDs to sanity check.

        :rtype: list of str:
        :returns: list of IDs
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
            exception_string = 'Sanity check failed: only found %s / %s expected results in table %s' % \
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
    Exception thrown by DynamoDBTask subclasses.
    """
    pass
