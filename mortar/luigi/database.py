import luigi
import datetime
from luigi import configuration, LocalTarget
from luigi.s3 import S3Target, S3PathTask, S3Client

"""
Generic task to move database to local target and vice-versa
"""
class DatabaseTask(luigi.Task):
    # local file targets
    local_path = configuration.get_config().get('transfers', 'local_path')
    file_name = configuration.get_config().get('transfers', 'file_name')

    # database parameters       
    # ideally, these are generic enough for any database connection
    host = configuration.get_config().get('database', 'host')
    database = configuration.get_config().get('database', 'database')
    user = configuration.get_config().get('database', 'user')
    password = configuration.get_config().get('database', 'password')
   # table = luigi.Parameter()
    table = configuration.get_config().get('database', 'table')

class DatabaseToLocalTask(DatabaseTask):
    def output(self):
        return LocalTarget(self.local_path + '/' + self.file_name)


class LocalToDatabaseTask(DatabaseTask):
    def input(self):
        return LocalTarget(self.local_path + '/' + self.file_name)
    


