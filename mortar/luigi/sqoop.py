import abc
import luigi, os
from luigi.s3 import S3PathTask
import logging

logger = logging.getLogger('luigi-interface')

class MortarSqoopTask(S3PathTask):
    """
    Base class for Mortar Sqoop commands
    Mortar Sqoop is a local tool to extract data from a JDBC database to an S3 bucket
    See http://help.mortardata.com/integrations/sql_databases for more information on how
    to use sqoop

    One required parameter:
        path - s3n path where results are stored
    Additional requirements are database configuration
    set in the client.cfg file.  
    Example:
    [database]
    dbtype: ${example: postgres}
    database: ${example: mydatabase}
    host: ${example: localhost}
    port: ${example: 1234}
    username: ${example: myusername}
    password: ${example: mypassword}

    Three optional parameters:
        jdbc_driver = Name of the JDBC driver class (example: COM.DRIVER.BAR)
        direct = Pass in True as a boolean to use native db tools instead of jdbc queries (hardly used)
        driver_jar = Path to the jar containing the jdbc driver (example: file://path/to/driver)

    """

    # path is a parameter
    jdbc_driver = luigi.Parameter(default=None)
    direct = luigi.Parameter(default=None)
    driver_jar = luigi.Parameter(default=None)
        
    
    def parameters(self):
        """
        These values are required parameters
        Set them in the client.cfg file
        """
        config = luigi.configuration.get_config()
        return {
                'dbtype' : config.get('database', 'dbtype'), # example (postgres, mysql, etc)
                'database' : config.get('database', 'database'), # example(mydatabase)
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

    def _append_array_if_item_exists(self, array, item, value):
        if item != None:
            array.append(value) 


    def run(self):
        params = self.parameters()
        command_arr = [
            'mortar',
            'local:%s' % self.command(),
            params['dbtype'],
            params['database'],
            self.arguments(),
            self.path,
            '-u %s' % params['username'],
            '-p %s' % params['password'],
            '--host %s' % (params['host'] + ':' + params['port'] if params['port'] != '' else params['host'])]

        self._append_array_if_item_exists(command_arr, 
                                          self.driver_jar, 
                                          '-r %s' % self.driver_jar)
        self._append_array_if_item_exists(command_arr, 
                                          self.direct, 
                                          '--direct')
        self._append_array_if_item_exists(command_arr, 
                                         self.jdbc_driver, 
                                         '-j %s' % self.jdbc_driver)

        logger.debug(command_arr)
        command_str = ' '.join(command_arr)
        self.command_str = command_str
        self.set_aws_keys()
        logger.debug(command_str)
        os.system(command_str)


class MortarSqoopQueryTask(MortarSqoopTask):
    """
    Export the result of an SQL query to S3.
    Runs:
    mortar local:sqoop_query dbtype database-name query path 

    sql_query must be implemented with the query that is going to be run
    Example:
    def sql_query(self):
        return 'select user_id, name from user'

    Required Parameters:
        path = s3n path to where data will be stored
    """

    def command(self):
        return 'sqoop_query'
    
    def arguments(self):
        return "'%s'" % self.sql_query()

    @abc.abstractmethod
    def sql_query(self):
        raise RuntimeError("must implement sql query!")

class MortarSqoopIncrementalTask(MortarSqoopTask):
    """
    Export all records where column is > value
    Runs:
    mortar local:sqoop_incremental dbtype database-name table column value path 

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
    mortar local:sqoop_table dbtype database-name table path 

    Required Parameter:
        path = s3n path to where data will be stored
        table = table you are extracting from
    """
    table = luigi.Parameter()

    def command(self):
        return 'sqoop_table'

    def arguments(self):
        return self.table


