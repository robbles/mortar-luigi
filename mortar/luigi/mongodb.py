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

import luigi
import logging
from mortar.luigi import target_factory

from pymongo import MongoClient

logger = logging.getLogger('luigi-interface')


class MongoDBTask(luigi.Task):
    """
    Class for tasks interacting with MongoDB.
    """

    @abc.abstractmethod
    def collection_name(self):
        """
        Name of the collection
        """
        raise RuntimeError("Must provide a collection_name")

    @abc.abstractmethod
    def output_token(self):
        """
        Token to be written out on completion of the task.
        """
        raise RuntimeError("Must provide an output token")

    def output(self):
        return self.output_token()


class SanityTestMongoDBCollection(MongoDBTask):
    """
    General check that the contents of a MongoDB collection exist and contain sentinel ids.
    """

    # number of entries required to be in the collection
    min_total_results = luigi.IntParameter(100)

    # when testing total entries, require that these field names not be null
    non_null_fields = luigi.Parameter([])

    # number of results required to be returned for each primary key
    result_length = luigi.IntParameter(5)

    # when testing specific ids, how many are allowed to fail
    failure_threshold = luigi.IntParameter(2)

    @abc.abstractmethod
    def ids(self):
        """
        Sentinel ids to check
        """
        return RuntimeError("Must provide list of ids to sanity test")


    def run(self):
        """
        Run sanity check.
        """
        col = self._get_collection()

        # check that the collection contains at least min_total_results entries
        fields = []
        for field in self.non_null_fields:
            fields.append({field, None})

        if fields:
            limit = self.min_total_results
            num_results = col.find({"$and":fields}).limit(limit).count(True)
            if num_results < limit:
                exception_string = 'Sanity check failed: only found %s / %s expected results in collection %s' % \
                    (num_results, limit, self.collection_name())
                logger.warn(exception_string)
                raise MongoDBTaskException(exception_string)

        # do a check on specific ids
        self._sanity_check_ids(col)

        # write token to note completion
        target_factory.write_file(self.output_token())

    def _get_collection(self):
        mongo_conn = luigi.configuration.get_config().get('mongodb', 'mongo_conn')
        mongo_db = luigi.configuration.get_config().get('mongodb', 'mongo_db')

        mc = MongoClient("%s/%s" % (mongo_conn, mongo_db))
        db = mc[mongo_db]
        return db[self.collection_name()]

    def _sanity_check_ids(self, collection):
        failure_count = 0
        for id in self.ids():
            num_results = collection.find({self.id_field:id}).limit(self.result_length).count(True)
            if num_results < self.result_length:
                failure_count += 1
                logger.info("Id %s only returned %s results." % (id, num_results))
        if failure_count > self.failure_threshold:
            exception_string = 'Sanity check failed: %s ids in %s failed to return sufficient results' % \
                        (failure_count, self.collection_name())
            logger.warn(exception_string)
            raise MongoDBTaskException(exception_string)


class MongoDBTaskException(Exception):
    """
    Exception thrown by MongoDBTasks
    """
    pass
