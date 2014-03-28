import luigi
import os
import abc 
import sys
import subprocess 
import logging
from luigi import LocalTarget
from mortar.luigi import target_factory

logger = logging.getLogger('luigi-interface')
class ShellScriptTask(luigi.Task):
    token_path = luigi.Parameter()

    def output_token(self):
        """
        Token written out to indicate finished shell script
        """
        return target_factory.get_target('%s/%s' % (self.token_path, self.__class__.__name__))

    def output(self):
        return [self.output_token()]

    @abc.abstractmethod
    def subprocess_commands(self):
        """
        Shell commands that will be ran in a subprocess
        Should return a string where each line of script is seperated with ';'
        Example:
            cd my/dir; ls;
        """
        raise RuntimeError("Must implement subprocess_commands!")
    
    def run(self):
        cmd = self.subprocess_commands()
        output = subprocess.Popen(
            cmd,
            shell=True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )
        out, err = output.communicate()
        # generate output message
        message = self._create_message(cmd, out, err)
        self._check_error(err, message)

        self.cmd_output = {
          'cmd'   : cmd,
          'stdout': out,
          'stderr': err
        }
        logger.debug('%s - output:%s' % (self.__class__.__name__, message))
        if err == '':
            target_factory.write_file(self.output_token())

    def _create_message(self, cmd, out, err):
        message = ''
        message += '\n-----------------------------'
        message += '\nCMD    : %s' % cmd
        message += '\nSTDOUT : %s' % repr(out)
        message += '\nSTDERR : %s' % repr(err)
        message += '\n-----------------------------'
        return message

    def _check_error(self, err, message):
        if err != '':
            raise RuntimeError(message)
            
