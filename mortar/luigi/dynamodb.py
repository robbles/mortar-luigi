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
import time

import boto.dynamodb2
from boto.dynamodb2.exceptions import DynamoDBError
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER, STRING

from luigi import configuration

import logging
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
            aws_access_key_id = configuration.get_config().get('dynamodb', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = configuration.get_config().get('dynamodb', 'aws_secret_access_key')
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
