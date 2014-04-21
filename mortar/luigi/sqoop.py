import abc
import luigi, os
from luigi.s3 import S3PathTask
import logging

logger = logging.getLogger('luigi-interface')

class MortarSqoopTask(S3PathTask):
    """
    Base class for Mortar Sqoop commands
    One required parameter:
        path - s3n path where results are stored
    Three optional parameters:
        jdbc_driver = Name of the JDBC driver class (example: COM.DRIVER.BAR)
        direct =   Use a direct import path (example: file://path/to/data)
        driver_jar = Path to the jar containing the jdbc driver (example: file://path/to/driver)
    """

    # path is a parameter
    jdbc_driver = luigi.Parameter(default=None)
    direct = luigi.Parameter(default=None)
    driver_jar = luigi.Parameter(default=None)
        
    def parameters(self):
        config = luigi.configuration.get_config()
        return {
                'dbtype' : config.get('database', 'dbtype'),
                'database' : config.get('database', 'database'),
                'host' : config.get('database', 'host'),
                'port' : config.get('database', 'port', ''),
                'username' : config.get('database', 'username'),
                'password' : config.get('database', 'password')}

    def aws_params(self):
        config = luigi.configuration.get_config()
        return {
                'aws_access_key_id' : config.get('s3', 'aws_access_key_id'),
                'aws_secret_access_key' : config.get('s3', 'aws_secret_access_key')}

    @abc.abstractmethod
    def command(self):
        raise RuntimeError("must implement command!")

    @abc.abstractmethod
    def arguments(self):
        raise RuntimeError("must implement arguments!")

    def options(self):
        return ''

    def set_aws_keys(self):
        aws_params = self.aws_params()
        os.environ['AWS_ACCESS_KEY'] = aws_params['aws_access_key_id']
        os.environ['AWS_SECRET_KEY'] = aws_params['aws_secret_access_key']

    def _append_array_if_item_exists(self, array, item, option_key):
        if item != None:
            array.append('-%s %s' % (option_key, item)) 


    def run(self):
        params = self.parameters()
        command_str = [
            'mortar',
            'local:%s' % self.command(),
            params['dbtype'],
            params['database'],
            self.arguments(),
            self.path,
            '-u %s' % params['username'],
            '-p %s' % params['password'],
            '--host %s' % (params['host'] + ':' + params['port'] if params['port'] != '' else params['host'])]

        self._append_array_if_item_exists(command_str, self.driver_jar, 'r')
        self._append_array_if_item_exists(command_str, self.direct, 'd')
        self._append_array_if_item_exists(command_str, self.jdbc_driver, 'j')

        command_str = ' '.join(command_str)
        self.command_str = command_str
        self.set_aws_keys()
        logger.debug(command_str)
        os.system(command_str)


class MortarSqoopQueryTask(MortarSqoopTask):
    """
    Export the result of an SQL query to S3.
    Runs:
    mortar local:sqoop_query dbtype database-name query s3-destination 

    sql_query must be implemented with the query that is going to be run
    Required Parameters:
        path = s3n path to where data will be stored
    """

    def command(self):
        return 'sqoop_query'
    
    def arguments(self):
        return '"%s"' % self.sql_query()

    @abc.abstractmethod
    def sql_query(self):
        raise RuntimeError("must implement sql query!")

class MortarSqoopIncrementalTask(MortarSqoopTask):
    """
    Export all records where column is > value
    Runs:
    mortar local:sqoop_incremental dbtype database-name table column value s3-destination

    Required Parameters:
        path = s3n path to where data will be stored
        table = table you are extracting from
        column = column of table you are comparing against
        value = minimum threshold of column
    """
    table = luigi.Parameter()
    column = luigi.Parameter()
    value = luigi.Parameter()

    def command(self):
        return 'sqoop_incremental'

    def arguments(self):
        return '%s %s %s' % (self.table, self.column, self.value)

class MortarSqoopTableTask(MortarSqoopTask):
    """
    Export all data from an RDBMS table to S3.
    Runs:
    mortar local:sqoop_table dbtype database-name table s3-destination

    Required Parameter:
        path = s3n path to where data will be stored
        table = table you are extracting from
    """
    table = luigi.Parameter()

    def command(self):
        return 'sqoop_table'

    def arguments(self):
        return self.table


