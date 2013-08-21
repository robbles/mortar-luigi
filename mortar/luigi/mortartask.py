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

import luigi
import luigi.configuration

from mortar.api.v2 import API
from mortar.api.v2 import clusters
from mortar.api.v2 import jobs

import logging
logger = logging.getLogger('luigi-interface')

class MortarProjectTask(luigi.Task):
    
    # default to a cluster of size 2
    cluster_size = luigi.IntParameter(default=2)
    
    # default to a single-job cluster
    cluster_type = luigi.Parameter(default=clusters.CLUSTER_TYPE_SINGLE_JOB)
    
    # run on master by default
    git_ref = luigi.Parameter(default='master')
    
    # Whether to notify on completion of a job
    notify_on_job_finish = luigi.BooleanParameter(default=False)
    
    # interval (in seconds) to poll for job status
    job_polling_interval = luigi.IntParameter(default=5)
        
    @abc.abstractmethod
    def project(self):
        """
        Name of the mortar project to run.
        """
        raise RuntimeError("Must implement project!")

    @abc.abstractmethod
    def script(self):
        """
        Name of the script to run.
        """
        raise RuntimeError("Must implement script!")

    @abc.abstractmethod
    def is_control_script(self):
        """
        Whether this job is a control script.
        """
        raise RuntimeError("Must implement is_control_script!")

    def parameters(self):
        """
        Parameters for this Mortar job.
        """
        return {}

    def run(self):
        """
        Run the mortar job.
        """
        # TODO: parameterize new cluster vs existing cluster
        api = self._get_api()
        job_id = self._run_job(api)
        
        job = self._poll_job_completion(api, job_id)
        final_job_status_code = job.get('status_code')
        if final_job_status_code != jobs.STATUS_SUCCESS:
            raise Exception('Mortar job_id [%s] failed with status_code: [%s], error details: %s' % (job_id, final_job_status_code, job.get('error')))
        else:
            logger.info('Mortar job_id [%s] completed successfully' % job_id)

    def _get_api(self):
        return API(luigi.configuration.get_config().get('mortar', 'email'),
                   luigi.configuration.get_config().get('mortar', 'api_key'))

    def _run_job(self, api):
        job_id = jobs.post_job_new_cluster(api, self.project(), self.script(), self.cluster_size, 
            cluster_type=self.cluster_type, git_ref=self.git_ref, parameters=self.parameters(),
            notify_on_job_finish=self.notify_on_job_finish, is_control_script=self.is_control_script())
        logger.info('Submitted new job to mortar with job_id [%s]' % job_id)
        return job_id
    
    def _poll_job_completion(self, api, job_id):
        
        current_job_status = None
        current_progress = None
        
        while True:
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
                # sleep and continue polling
                time.sleep(self.job_polling_interval)
    
    def _get_job_status_description(self, job):
        desc = job.get('status_description')
        if job.get('status_details'):
            desc += ' - %s' % job.get('status_details')
        return desc
        
class MortarProjectPigscriptTask(MortarProjectTask):
    
    def is_control_script(self):
        return False

class MortarProjectControlscriptTask(MortarProjectTask):

    def is_control_script(self):
        return True

