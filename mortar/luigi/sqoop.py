import abc
import luigi, os
from luigi.s3 import S3PathTask
import logging

logger = logging.getLogger('luigi-interface')

class MortarSqoopTask(S3PathTask):
    # path is a parameter
        
    def parameters(self):
        config = luigi.configuration.get_config()
        return {
                'dbtype' : config.get('database', 'dbtype'),
                'database' : config.get('database', 'database'),
                'host' : config.get('database', 'host'),
                'port' : config.get('database', 'port', ''),
                'username' : config.get('database', 'username'),
                'password' : config.get('database', 'password')}

    @abc.abstractmethod
    def command(self):
        raise RuntimeError("must implement command!")

    @abc.abstractmethod
    def arguments(self):
        raise RuntimeError("must implement arguments!")

    def options(self):
        return ''

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
            '--host %s' % (params['host'] + ':' + params['port']) if params['port'] != '' else params['host']]

        command_str = ' '.join(command_str)
        self.command_str = command_str
        logger.debug(command_str)
        os.system(command_str)


class MortarSqoopQueryTask(MortarSqoopTask):
    def command(self):
        return 'sqoop_query'
    
    def arguments(self):
        return self.sql_query()

    @abc.abstractmethod
    def sql_query(self):
        raise RuntimeError("must implement sql query!")

class MortarSqoopIncrementalTask(MortarSqoopTask):
    table = luigi.Parameter()
    column = luigi.Parameter()
    value = luigi.Parameter()

    def command(self):
        return 'sqoop_incremental'

    def arguments(self):
        return '%s %s %s' % (self.table, self.column, self.value)

class MortarSqoopTableTask(MortarSqoopTask):
    table = luigi.Parameter()

    def command(self):
        return 'sqoop_table'

    def arguments(self):
        return self.table


