# Copyright (c) 2014 Mortar Data
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

import luigi
import abc
import subprocess 
import logging
from mortar.luigi import target_factory

logger = logging.getLogger('luigi-interface')

class ShellScriptTask(luigi.Task):
    """
    Luigi Task to run a shell script.

    seealso:: https://help.mortardata.com/technologies/luigi/shellscript_tasks
    """

    # path to write token to indicate that script has been run
    # e.g. s3://my-bucket/my-path
    token_path = luigi.Parameter()

    def output_token(self):
        """
        Luigi Target providing path to a token that indicates
        completion of this Task.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        return target_factory.get_target('%s/%s' % (self.token_path, self.__class__.__name__))

    def output(self):
        """
        The output for this Task. Returns the output token
        by default, so the task only runs if the token does not 
        already exist.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        return [self.output_token()]

    @abc.abstractmethod
    def subprocess_commands(self):
        """
        Override this method to provide shell commands 
        that will be run in a subprocess.

        You should return a string where each line of script is separated with ';'
        Example:
        
        ::cd my/dir; ls;

        :rtype: str:
        :returns: Shell commands to run in subprocess. e.g. "cd my/dir; ls;"
        """
        raise RuntimeError("Please implement subprocess_commands method")
    
    def run(self):
        cmd = self.subprocess_commands()
        output = subprocess.Popen(
            cmd,
            shell=True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )
        out, err = output.communicate()
        rc = output.returncode
        # generate output message
        message = self._create_message(cmd, out, err, rc)
        self._check_error(rc, err, message)

        self.cmd_output = {
          'cmd'         : cmd,
          'stdout'      : out,
          'stderr'      : err,
          'return_code' : rc
        }
        logger.debug('%s - output:%s' % (self.__class__.__name__, message))
        if err == '':
            target_factory.write_file(self.output_token())

    def _create_message(self, cmd, out, err, rc):
        message = ''
        message += '\n-----------------------------'
        message += '\nCMD         : %s' % cmd
        message += '\nSTDOUT      : %s' % repr(out)
        message += '\nSTDERR      : %s' % repr(err)
        message += '\nRETURN CODE : %d' % rc
        message += '\n-----------------------------'
        return message

    def _check_error(self, rc, err, message):
        if err != '' or rc != 0:
            raise RuntimeError(message)
            
