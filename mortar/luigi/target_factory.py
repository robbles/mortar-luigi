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
from luigi.s3 import S3Target
from luigi.target import FileSystemTarget


def get_target(path):
    """
    Factory method for creating a target from a path.
    """
    if path.startswith('s3:'):
        return S3Target(path)
    elif path.startswith('file:'):
        return FileSystemTarget(path)
    else:
        raise RuntimeError("Unknown target path type: %s" % path)