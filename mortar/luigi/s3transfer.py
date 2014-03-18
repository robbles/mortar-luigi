import luigi, abc
from luigi.parameter import Parameter
import datetime
from luigi import configuration, LocalTarget
from luigi.s3 import S3Target, S3PathTask, S3Client
from mortar.luigi import target_factory


"""
Generic task to move s3 to local target and vice-versa
    requires definition of input() and output()
"""
class S3TransferTask(luigi.Task):
    # s3 path to where file should be/go
    s3_path = Parameter()
    local_path = Parameter()
    file_name = Parameter()

    def get_aws_keys(self):
        if not hasattr(self, "aws_access_key_id") and  not hasattr(self, "aws_secret_access_key") :
            self.aws_access_key_id = configuration.get_config().get('s3', 'aws_s3_access_key_id')
            self.aws_secret_access_key = configuration.get_config().get('s3', 'aws_s3_secret_access_key')
        return [self.aws_access_key_id, self.aws_secret_access_key]

    def output(self):
        return [self.success_token()]

    def running_token(self):
        """
        Token written out to indicate a running Pigscript
        """
        return target_factory.get_target('%s/%s-%s' % (self.token_path(), self.__class__.__name__, 'Running'))

    def success_token(self):
        """
        Token written out to indicate the Pigscript has finished
        """
        return target_factory.get_target('%s/%s' % (self.token_path(), self.__class__.__name__))

    @abc.abstractmethod
    def token_path(self):
        raise RuntimeError("Must implement token_path!")

    @abc.abstractmethod
    def input_file(self):
        raise RuntimeError("Must implement input_file!")

    @abc.abstractmethod
    def output_file(self):
        raise RuntimeError("Must implement output_file!")


    def run(self):
      #  if self.running_token().exists():
      #      self.running_token().remove()
     #   target_factory.write_file(self.running_token())
        r = self.input_file().open('r')
        w = self.output_file().open('w')
        w.write(r.readline())
        w.close()
            
        target_factory.write_file(self.success_token())

        # check if success exists
          #if does, move on
          # if not, check if running exists
            # if yes, find current line, start writing
            # else, start writing at beginning



class LocalToS3Task(S3TransferTask):

    def token_path(self):
        return self.local_path

    def input_file(self):
        return LocalTarget(self.local_path + '/' + self.file_name)

    def output_file(self):
        aws_access_key_id, aws_secret_access_key = self.get_aws_keys()
        return S3Target(self.s3_path + '/' + self.file_name, client=S3Client(aws_access_key_id, aws_secret_access_key))


class S3ToLocalTask(S3TransferTask):
    def token_path(self):
        return self.local_path

    def output_file(self):
        return LocalTarget(self.local_path + '/' + self.file_name)

    def input_file(self):
        aws_access_key_id, aws_secret_access_key = self.get_aws_keys()
        return S3Target(self.s3_path + '/' + self.file_name, client=S3Client(aws_access_key_id, aws_secret_access_key))


