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
import os
import subprocess
import tempfile
import time

import luigi


from mortar.api.v2 import API
from mortar.api.v2 import clusters
from mortar.api.v2 import jobs

import logging
from mortar.luigi import target_factory

logger = logging.getLogger('luigi-interface')

# Number of map task slots per m1.xlarge instance
NUM_MAP_SLOTS_PER_MACHINE = 8

# Number of reduce task slots per m1.xlarge instance
NUM_REDUCE_SLOTS_PER_MACHINE = 3

# flag to indicate that no git_ref has been passed to a mortar task method
NO_GIT_REF_FLAG = "not-set-flag"

class MortarTask(luigi.Task):
    """
    Superclass for Luigi Tasks that perform actions in Mortar
    using the Mortar API.

    To use this class, define the following section in your Luigi 
    client configuration file:

    ::[mortar]
    ::email: ${MORTAR_EMAIL}
    ::api_key: ${MORTAR_API_KEY}
    ::host: api.mortardata.com

    seealso:: https://help.mortardata.com/technologies/luigi/mortar_tasks
    """

    def _get_api(self):
        config = luigi.configuration.get_config()
        email = config.get('mortar', 'email')
        api_key = config.get('mortar', 'api_key')
        if config.has_option('mortar', 'host'):
            host = config.get('mortar', 'host')
            return API(email, api_key, host=host)
        else:
            return API(email, api_key)

class MortarProjectTask(MortarTask):
    """
    Luigi Task to run a job on the Mortar platform. 
    If the job fails, the task will exit with an error.

    To use this class, define the following section in your Luigi 
    client configuration file:

    ::[mortar]
    ::email: ${MORTAR_EMAIL}
    ::api_key: ${MORTAR_API_KEY}
    ::host: api.mortardata.com
    ::project_name: ${MORTAR_PROJECT_NAME}

    see also:: https://help.mortardata.com/technologies/luigi/mortar_tasks
    """

    # A cluster size of 2 or greater will use a Hadoop cluster.  If there
    # is an idle cluster of cluster_size or greater that cluster will be used.
    # Otherwise a new cluster will be started.
    # A cluster size of 0 will run the Mortar job directly on the Mortar Pig
    # server in local mode (no cluster).
    # All other cluster_size values are invalid.
    cluster_size = luigi.IntParameter(default=2)


    # A single use cluster will be terminated immediately after this
    # Mortar job completes.  Otherwise it will be terminated automatically
    # after being idle for one hour.
    # This option does not apply when running the Mortar job in local mode
    # (cluster_size = 0).
    run_on_single_use_cluster = luigi.BooleanParameter(False)

    # If False, this task will only run on an idle cluster or will
    # start up a new cluster if no idle clusters are found.  If True,
    # this task may run on a cluster that has other jobs already running on it.
    # If run_on_single_use_cluster is True, this parameter will be ignored.
    share_running_cluster = luigi.BooleanParameter(False)

    # Whether a launched Hadoop cluster will take advantage of AWS
    # Spot Pricing (https://help.mortardata.com/technologies/hadoop/spot_instance_clusters)
    # This option does not apply when running in local mode (cluster_size = 0).
    use_spot_instances = luigi.BooleanParameter(True)

    # The Git reference (commit hash or branch name) to use when running
    # this Mortar job.  The default value NO_GIT_REF_FLAG is a flag value
    # that indicates no value was entered as a parameter.  If no value
    # is passed as a parameter the environment value "MORTAR_LUIGI_GIT_REF"
    # is used.  If that is not set the "master" is used.
    git_ref = luigi.Parameter(default=NO_GIT_REF_FLAG)

    # Set to true to receive an email upon completion
    # of this Mortar job.
    notify_on_job_finish = luigi.BooleanParameter(default=False)

    # Internval (in seconds) to poll for job status.
    job_polling_interval = luigi.IntParameter(default=5)

    # Number of retries before giving up on polling.
    num_polling_retries = luigi.IntParameter(default=3)

    # Version of Pig to use.
    pig_version = luigi.Parameter(default='0.12')

    def project(self):
        """
        Override this method to provide the name of 
        the Mortar Project.

        :rtype: str:
        :returns: Your project name, e.g. my-mortar-recsys
        """
        if luigi.configuration.get_config().has_option('mortar', 'project_name'):
            project_name = luigi.configuration.get_config().get('mortar', 'project_name')
            return project_name
        raise RuntimeError("Please implement the project method or provide a project_name configuration item to return your project name")

    @abc.abstractmethod
    def script(self):
        """
        Override this method to provide the name of 
        the script to run.

        :rtype: str:
        :returns: Script name, e.g. my_pig_script
        """
        raise RuntimeError("Please implement the script method to return your script name")

    @abc.abstractmethod
    def is_control_script(self):
        """
        [DEPRECATED] Whether this job should run a control script.

        :rtype: bool:
        :returns: [DEPRECATED] whether this job should run a control script
        """
        raise RuntimeError("Please implement the is_control_script method")

    def parameters(self):
        """
        This method defines the parameters that Mortar will pass to your
        your script when it runs.

        :rtype: dict:
        :returns: dict of parameters to pass, e.g. {'my-param': 'my-value'}. Default: {}
        """
        return {}

    def output(self):
        """
        The output for this Task. Returns the `success_token`
        by default, so the Task only runs if a token indiciating success
        has not been previously written.

        :rtype: list of Target:
        :returns: list containing one output, the `success_token`
        """
        return [self.success_token()]

    def token_path(self):
        """
        The MortarProjectTask writes out several "tokens" as it executes, indicating
        whether it is Running and then when it is Complete. These tokens are
        used to ensure that the task is not rerun once it has already completed.

        This method provides the base path where those tokens are written. By default,
        tokens are written to a temporary directory on the file system.

        However, for running in a cluster setting, you should overrides this method
        to use an S3 path (e.g. s3://my-bucket/my-token-path), 
        ensuring that tokens will be available from any machine.

        :rtype: str:
        :returns: default token path on file system - file://tempdirectory
        """
        # override with S3 path for usage across machines or on clusters
        return 'file://%s' % tempfile.gettempdir()

    @abc.abstractmethod
    def script_output(self):
        """
        List of locations where your script writes output. If your script fails, Luigi
        will clear any output from these locations to ensure that the next run of your
        Task is idempotent.

        :rtype: list of Target:
        :returns: list of Target to clear in case of Task failure
        """
        raise RuntimeError("Please implement the script_output method")

    def running_token(self):
        """
        The MortarProjectTask writes out several "tokens" as it executes to ensure
        idempotence. This method provides the token file that indicates that the job 
        is running.

        By default, it is stored underneath the path provided by the `token_path` method,
        and is named after your class name. So, if your `token_path` is set to 
        `s3://my-bucket/my-folder` and your Task is named FooTask, the token will be:

        `s3://my-bucket/my-folder/FooTask-Running`

        This token will contain the Mortar job_id of the job that is running.

        :rtype: Target:
        :returns: Target for the token that indicates job is running.
        """
        return target_factory.get_target('%s/%s-%s' % (self.token_path(), self.__class__.__name__, 'Running'))

    def success_token(self):
        """
        The MortarProjectTask writes out several "tokens" as it executes to ensure
        idempotence. This method provides the token file that indicates that the job 
        has finished successfully. If this token exists, the Task will not be rerun.

        By default, it is stored underneath the path provided by the `token_path` method,
        and is named after your class name. So, if your `token_path` is set to 
        `s3://my-bucket/my-folder` and your Task is named FooTask, the token will be:

        `s3://my-bucket/my-folder/FooTask`

        If you want this Task to be rerun, you should delete that token.

        :rtype: Target:
        :returns: Target for the token that indicates that this Task has succeeded.
        """
        return target_factory.get_target('%s/%s' % (self.token_path(), self.__class__.__name__))

    def run(self):
        """
        Run a Mortar job using the Mortar API.

        This method writes out several "tokens" as it executes to ensure
        idempotence:

        * `running_token`: This token indicates that the job is currently running. If a token
          exists at this path, Luigi will poll the currently running job instead of starting a 
          new one.
        * `success_token`: This token indicates that the job has already completed successfully.
          If this token exists, Luigi will not rerun the task.
        """
        api = self._get_api()
        if self.running_token().exists():
            job_id = self.running_token().open().read().strip()
        else:
            job_id = self._run_job(api)
            # to guarantee idempotence, record that the job is running
            target_factory.write_file(self.running_token(), text=job_id)
        job = self._poll_job_completion(api, job_id)
        final_job_status_code = job.get('status_code')
        # record that the job has finished
        self.running_token().remove()
        if final_job_status_code != jobs.STATUS_SUCCESS:
            for out in self.script_output():
                logger.info('Mortar script failed: removing incomplete data in %s' % out)
                out.remove()
            raise Exception('Mortar job_id [%s] failed with status_code: [%s], error details: %s' % (job_id, final_job_status_code, job.get('error')))
        else:
            target_factory.write_file(self.success_token())
            logger.info('Mortar job_id [%s] completed successfully' % job_id)

    def _git_ref(self):
        """
        Figure out value to use for git ref.  Order of precendence is:

        1. git_ref parameter is set.
        2. environment variable MORTAR_LUIGI_GIT_REF is set
        3. master
        """
        if self.git_ref != NO_GIT_REF_FLAG:
            return self.git_ref
        else:
            import os
            env_git_ref = os.environ.get('MORTAR_LUIGI_GIT_REF')
            if env_git_ref:
                return env_git_ref
            else:
                return 'master'


    def _run_job(self, api):
        cluster_type = clusters.CLUSTER_TYPE_SINGLE_JOB if self.run_on_single_use_cluster \
            else clusters.CLUSTER_TYPE_PERSISTENT
        cluster_id = None
        if self.cluster_size == 0:
            # Use local cluster
            cluster_id = clusters.LOCAL_CLUSTER_ID
        elif not self.run_on_single_use_cluster:
            # search for a suitable cluster
            usable_clusters = self._get_usable_clusters(api, min_size=self.cluster_size)
            if usable_clusters:
                # grab the largest usable cluster
                largest_cluster = sorted(usable_clusters, key=lambda c: int(c['size']), reverse=True)[0]
                logger.info('Using largest running usable cluster with cluster_id [%s], size [%s]' % \
                    (largest_cluster['cluster_id'], largest_cluster['size']))
                cluster_id = largest_cluster['cluster_id']


        if cluster_id:
            job_id = jobs.post_job_existing_cluster(api, self.project(), self.script(), cluster_id,
                git_ref=self._git_ref(), parameters=self.parameters(),
                notify_on_job_finish=self.notify_on_job_finish, is_control_script=self.is_control_script(),
                pig_version=self.pig_version, pipeline_job_id=self._get_pipeline_job_id())
        else:
            job_id = jobs.post_job_new_cluster(api, self.project(), self.script(), self.cluster_size,
                cluster_type=cluster_type, git_ref=self._git_ref(), parameters=self.parameters(),
                notify_on_job_finish=self.notify_on_job_finish, is_control_script=self.is_control_script(),
                pig_version=self.pig_version, use_spot_instances=self.use_spot_instances, 
                pipeline_job_id=self._get_pipeline_job_id())
        logger.info('Submitted new job to mortar with job_id [%s]' % job_id)
        return job_id

    def _get_usable_clusters(self, api, min_size=0):
        return [cluster for cluster in clusters.get_clusters(api)['clusters'] \
            if (    (cluster.get('status_code') == clusters.CLUSTER_STATUS_RUNNING)
                and (cluster.get('cluster_type_code') != clusters.CLUSTER_TYPE_SINGLE_JOB)
                and (int(cluster.get('size')) >= min_size)
                and (    len(cluster.get('running_jobs')) == 0
                      or self.share_running_cluster)
               )]

    def _poll_job_completion(self, api, job_id):

        current_job_status = None
        current_progress = None

        exception_count = 0
        while True:
            try:
                # fetch job
                job = jobs.get_job(api, job_id)
                new_job_status = job.get('status_code')



                # check for updated status
                if new_job_status != current_job_status:
                    current_job_status = new_job_status
                    logger.info('Mortar job_id [%s] switched to status_code [%s], description: %s' % \
                        (job_id, new_job_status, self._get_job_status_description(job)))

                # check for updated progress on running job
                if (new_job_status == jobs.STATUS_RUNNING) and (job.get('progress') != current_progress):
                    current_progress = job.get('progress')
                    logger.info('Mortar job_id [%s] progress: [%s%%]' % (job_id, current_progress))

                # final state
                if current_job_status in jobs.COMPLETE_STATUSES:
                    return job
                else:
                    # reset exception count on successful loop
                    exception_count = 0

                    # sleep and continue polling
                    time.sleep(self.job_polling_interval)
            except Exception, e:
                if exception_count < self.num_polling_retries:
                    exception_count += 1
                    logger.info('Failure to get job status for job %s: %s' % (job_id, str(e)))
                    time.sleep(self.job_polling_interval)
                else:
                    raise

    def _get_job_status_description(self, job):
        desc = job.get('status_description')
        if job.get('status_details'):
            desc += ' - %s' % job.get('status_details')
        return desc

    def _get_pipeline_job_id(self):
        """
        Get the job_id for the parent luigi pipeline 
        that executed this Pig job, if applicable.
        """
        return os.environ.get('PIPELINE_JOB_ID')


class MortarProjectPigscriptTask(MortarProjectTask):
    """
    Luigi Task to run a Pigscript on the Mortar platform. 
    If the job fails, the task will exit with an error.

    To use this class, define the following section in your Luigi 
    client configuration file:

    ::[mortar]
    ::email: ${MORTAR_EMAIL}
    ::api_key: ${MORTAR_API_KEY}
    ::host: api.mortardata.com

    seealso:: https://help.mortardata.com/technologies/luigi/mortar_tasks
    """

    def is_control_script(self):
        return False

class MortarProjectControlscriptTask(MortarProjectTask):
    """
    [DEPRECATED]

    Luigi Task to run a Pig-based control script on the Mortar platform. 
    If the job fails, the task will exit with an error.
    """

    def is_control_script(self):
        return True

class MortarRTask(luigi.Task):
    """
    Luigi Task to run an R script.

    To use this Task in a pipeline, create a subclass that overrides the methods:

    * `rscript`
    * `arguments`

    seealso:: https://help.mortardata.com/technologies/luigi/r_tasks
    """

    # Location where completion tokens are written
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
    def rscript(self):
        """
        Path to the R script to run, relative to the root of your Mortar project.

        Ex:
            If you have two files in your Mortar project:
                * luigiscripts/my_r_luigiscript.py
                * rscripts/my_r_script.R

            You would return:
                "rscripts/my_r_script.R"

        :rtype: str:
        :returns: Path to your R script relative to the root of your Mortar project. e.g. rscripts/my_r_script.R
        """
        raise RuntimeError("Please implement the rscript method in your MortarRTask to specify which script to run.")

    def arguments(self):
        """
        Returns list of arguments to be sent to your R script.

        :rtype: list of str:
        :returns: List of arguments to pass to your R script. Default: []
        """
        return []

    def run(self):
        """
        Run an R script using the Rscript program. Pipes stdout and
        stderr back to the logging facility.
        """
        cmd = self._subprocess_command()
        output = subprocess.Popen(
            cmd,
            shell=True,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
            bufsize=1
        )
        for line in iter(output.stdout.readline, b''):
            logger.info(line)
        out, err = output.communicate()
        rc = output.returncode
        if rc != 0:
            raise RuntimeError('%s returned non-zero error code %s' % (self._subprocess_command(), rc) )

        target_factory.write_file(self.output_token())

    def _subprocess_command(self):
       return "Rscript %s %s" % (self.rscript(), " ".join(self.arguments()))


class MortarClusterShutdownTask(MortarTask):
    """
    Luigi Task to shuts down all running clusters 
    without active jobs for the specified user.

    seealso:: https://help.mortardata.com/technologies/luigi/cluster_management_tasks
    """

    def _get_running_idle_clusters(self, api):
        return [c for c in clusters.get_clusters(api).get('clusters') if not c.get('running_jobs')
            and c.get('status_code') == clusters.CLUSTER_STATUS_RUNNING]

    def run(self):
        """
        Shut down all running clusters without active jobs.
        """
        api = self._get_api()
        active_clusters = self._get_running_idle_clusters(api)
        for c in active_clusters:
            logger.info('Stopping idle cluster %s' % c.get('cluster_id'))
            clusters.stop_cluster(api, c.get('cluster_id'))


