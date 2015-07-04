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
import subprocess

import luigi
import logging
from mortar.luigi import target_factory

logger = logging.getLogger('luigi-interface')


class DBMSTask(luigi.Task):
    """
    Superclass for Luigi Tasks interacting with SQL Databases.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table on which operation should be performed.

        :rtype: str:
        :returns: table_name for operation
        """
        raise RuntimeError("Please implement the table_name method in your DBMSTask subclass")


    @abc.abstractmethod
    def output_token(self):
        """
        Luigi Target providing path to a token that indicates
        completion of this Task.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        raise RuntimeError("Please implement the output_token method in your DBMSTask subclass")

    def output(self):
        """
        The output for this Task. Returns the output token
        by default, so the task only runs if the token does not 
        already exist.

        :rtype: Target:
        :returns: Target for Task completion token
        """
        return self.output_token()

    conn = None

    def get_connection(self):
        """
        Get a connection to the database. Subclasses must implement
        this method to provide database connection.

        :rtype: Connection:
        :returns: Database connection
        """
        raise RuntimeError("Please implement the get_connection method in your DBMSTask subclass")


class PostgresTask(DBMSTask):
    """
    Superclass for Luigi Tasks that interact with a
    PostgreSQL database.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[postgres]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the psycopg2 module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """

    def get_connection(self):
        """
        Get a connection to the PostgreSQL database.

        :rtype: Connection:
        :returns: Database connection
        """
        import psycopg2
        import psycopg2.errorcodes
        import psycopg2.extensions

        if not self.conn:
            dbname = luigi.configuration.get_config().get('postgres', 'dbname')
            user = luigi.configuration.get_config().get('postgres', 'user')
            host = luigi.configuration.get_config().get('postgres', 'host')
            password = luigi.configuration.get_config().get('postgres', 'password')
            port = luigi.configuration.get_config().get('postgres', 'port')

            try:
                self.conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException(
                    "Unable to connect to database %s, host: %s, port: %s, user: %s" % \
                        (dbname, host, port, user))
        return self.conn

class MySQLTask(DBMSTask):
    """
    Superclass for Luigi Tasks that interact with a
    MySQL database.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[mysql]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the mysql-connector-python
    module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """

    def get_connection(self):
        """
        Get a connection to the MySQL database.

        :rtype: Connection:
        :returns: Database connection
        """
        import mysql.connector

        if not self.conn:
            dbname = luigi.configuration.get_config().get('mysql', 'dbname')
            user = luigi.configuration.get_config().get('mysql', 'user')
            host = luigi.configuration.get_config().get('mysql', 'host')
            password = luigi.configuration.get_config().get('mysql', 'password')
            port = luigi.configuration.get_config().get('mysql', 'port')

            try:
                self.conn = mysql.connector.connect(database=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException("Unable to connect to database %s" % dbname)
        return self.conn

class CreateDBMSTable(DBMSTask):
    """
    Superclass for Tasks to create a new table in a DBMS.
    Don't instantiate this directly; use the :py:class:`CreatePostgresTable`
    or :py:class:`CreateMySQLTable` instead.
    """
    def primary_key(self):
        """
        List of primary key columns to set for the new table.

        :rtype: list of str:
        :returns: list of primary key columns
        """
        raise RuntimeError("Please implement the primary_key method to provide primary key columns")

    def field_string(self):
        """
        String enumerating all fields and their data types, 
        including primary keys.

        e.g.: 'num integer, data varchar'

        :rtype: str:
        :returns: fields and data types to include in the table, comma-delimited
        """
        raise RuntimeError("Please implement the field_string method to provide fields for the table")

    def run(self):
        """
        Create database table.
        """
        connection = self.get_connection()
        cur = connection.cursor()
        table_query = self._create_table_query()
        cur.execute(table_query)
        connection.commit()
        cur.close()
        connection.close()
        # write token to acknowledge table creation
        target_factory.write_file(self.output_token())

    def _create_table_query(self):
        primary_keys = ','.join(self.primary_key())
        return 'CREATE TABLE %s(%s, PRIMARY KEY (%s));' % (self.table_name(), self.field_string(), primary_keys)


class SanityTestDBMSTable(DBMSTask):
    """
    Superclass for Tasks to sanity test that a set of IDs
    exist in a table (usually after loading it with data).
    """

    # minimum number of rows required to be in the table
    min_total_results = luigi.IntParameter(100)

    # number of results required to be returned for each ID
    result_length = luigi.IntParameter(5)

    # when testing specific IDs, how many are allowed to fail?
    failure_threshold = luigi.IntParameter(2)

    @abc.abstractmethod
    def id_field(self):
        """
        Name of the ID field to sanity check.

        :rtype: str:
        :returns: name of ID field to sanity check
        """
        raise RuntimeError("Please implement the id_field method")

    @abc.abstractmethod
    def ids(self):
        """
        List of IDs to sanity check.

        :rtype: list of str:
        :returns: list of IDs
        """
        return RuntimeError("Must provide list of ids to sanity test")

    def run(self):
        """
        Run a sanity check on the table, ensuring that
        data was loaded appropriately.  Raises a :py:class:`DBMSTaskException`
        if the sanity check fails.
        """
        cur = self.get_connection().cursor()
        overall_query = self._create_overall_query()
        cur.execute(overall_query)
        rows = cur.fetchall()

        if len(rows) < self.min_total_results:
            exception_string = 'Sanity check failed: only found %s / %s expected results in collection %s' % \
                (len(rows), self.min_total_results, self.table_name())
            logger.warn(exception_string)
            cur.close()
            self.get_connection().close()
            raise DBMSTaskException(exception_string)

        # do a check on specific ids
        self._sanity_check_ids()

        cur.close()
        self.get_connection().close()
        # write token to note completion
        target_factory.write_file(self.output_token())

    def _create_overall_query(self):
        limit_clause = ' LIMIT %s ' % self.min_total_results
        return 'SELECT * FROM %s %s' % (self.table_name(), limit_clause)

    def _create_id_query(self, id):
        return 'SELECT * FROM %s WHERE %s = \'%s\'' % (self.table_name(), self.id_field(), id)

    def _sanity_check_ids(self):
        failure_count = 0
        cur = self.get_connection().cursor()
        for id in self.ids():
            id_query = self._create_id_query(id)
            cur.execute(id_query)
            rows = cur.fetchall()
            num_results = len(rows)
            if num_results < self.result_length:
                failure_count += 1
                logger.info("Id %s only returned %s results." % (id, num_results))
        if failure_count > self.failure_threshold:
            exception_string = 'Sanity check failed: %s ids in %s failed to return sufficient results' % \
                        (failure_count, self.table_name())
            logger.warn(exception_string)
            raise DBMSTaskException(exception_string)


class CreatePostgresTable(CreateDBMSTable, PostgresTask):
    """
    Create a new table in a PostgreSQL database.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[postgres]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the psycopg2 module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """
    pass

class CreateMySQLTable(CreateDBMSTable, MySQLTask):
    """
    Create a new table in a MySQL database.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[mysql]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the mysql-connector-python
    module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """
    pass

class SanityTestPostgresTable(SanityTestDBMSTable, PostgresTask):
    """
    Run a sanity test that data has been properly loaded
    into a PostgreSQL database table.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[postgres]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the psycopg2 module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """
    pass

class SanityTestMySQLTable(SanityTestDBMSTable, MySQLTask):
    """
    To use this class, define the following section in your Luigi 
    configuration file:

    ::[mysql]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the mysql-connector-python
    module.

    seealso:: https://help.mortardata.com/technologies/luigi/dbms_tasks
    """
    pass

class ExtractFromMySQL(luigi.Task):
    """
    Extracts data from MySQL.

    To use this class, define the following section in your Luigi 
    configuration file:

    ::[mysql]
    ::dbname=my_database_name
    ::host=my_hostname
    ::port=my_port
    ::user=my_username
    ::password=my_password

    Also, ensure you have installed the mysql command-line tool
    on your system. This tool is installed automatically on 
    Mortar's pipeline servers.

    You can also override dbname and host by providing parameters
    to your Luigi task.
    """

    # Name of MySQL Database where table is located.
    # Default: mysql.dbname in your configuration file.
    dbname = luigi.Parameter(default=None)

    # Hostname for MySQL Database where table is located
    # Default: mysql.host in your configuration file.
    host = luigi.Parameter(default=None)

    # Table to extract
    table = luigi.Parameter()

    # Columns to select, in comma-delimited list.
    # e.g. "mycol1,mycol2,mycol3"
    #
    # NOTE: You should override the default with
    # an explicit column list to ensure correct ordering if
    # you add new columns to your table.
    columns = luigi.Parameter(default='*')

    # Custom WHERE clause (excluding the WHERE part).
    # Default: no where clause; all rows selected.
    # Example: 'mycol < 75'
    where = luigi.Parameter(default=None)

    # Output location for the extracted data.
    # Example: s3://my-path/my-output-path
    output_path = luigi.Parameter()

    # The mysql CLI exports the string "NULL" whenever a field
    # is NULL. If replace_null_with_blank is true, any occurrrence of
    # the string "NULL" in the extract will be replaced with a
    # blank string.
    # Default: True
    replace_null_with_blank = luigi.BooleanParameter(default=True)

    # Usually, the mysql CLI escapes control characters.
    # For example: newline, tab, NUL, and backslash are written as \n, \t, \0, and \\. 
    # If raw is set, this character escaping will be disabled.
    # Default: False
    raw = luigi.BooleanParameter(default=False)

    def user(self):
        config = luigi.configuration.get_config()
        return config.get('mysql', 'user')

    def password(self):
        config = luigi.configuration.get_config()
        return config.get('mysql', 'password')

    def port(self):
        config = luigi.configuration.get_config()
        return config.get('mysql', 'port')

    def output(self):
        """
        Tell Luigi about the output that this Task produces.
        If that output already exists, Luigi will not rerun it.
        """
        return [target_factory.get_target(self.output_path)]

    def run(self):
        config = luigi.configuration.get_config()
        dbname = self.dbname if self.dbname else config.get('mysql', 'dbname')
        mysql_host = self.host if self.host else config.get('mysql', 'host')
        mysql_user = self.user()
        mysql_password = self.password()
        mysql_port = self.port()

        where_clause = 'WHERE %s' % self.where if self.where else ''
        raw_option = '--raw' if self.raw else ''
        cmd_template = 'mysql --user="{user}" --password="{password}" --host="{host}" --port="{port}" --compress --reconnect --quick --skip-column-names {raw_option} -e "SELECT {columns} FROM {table} {where_clause}" "{dbname}"'
        if self.replace_null_with_blank:
            # replace explicit NULL characters with sed
            # and be sure to capture mysql failures by setting
            # pipefail
            cmd_template = 'set -o pipefail && ' +  cmd_template + ' | sed -e "s/NULL//g"'
        cmd = cmd_template.format(
            user=mysql_user,
            password=mysql_password,
            host=mysql_host,
            port=mysql_port,
            columns=self.columns,
            table=self.table,
            where_clause=where_clause,
            raw_option=raw_option,
            dbname=dbname)
        cmd_printable = cmd_template.format(
            user=mysql_user,
            password='[REDACTED]',
            host=mysql_host,
            port=mysql_port,
            columns=self.columns,
            table=self.table,
            where_clause=where_clause,
            raw_option=raw_option,
            dbname=dbname)

        # open up the target output to store data
        output_data_file = self.output()[0].open('w')
        logger.info('Extracting data from MySQL with command: %s' % cmd_printable)
        output = subprocess.Popen(
            cmd,
            shell=True,
            executable='/bin/bash',
            stdout = output_data_file,
            stderr = subprocess.PIPE,
            # buffer line-by-line to stdout
            bufsize=1
        )
        # report any error messages to logs
        for line in iter(output.stderr.readline, b''):
            if line and not 'Using a password' in line:
                logger.info(line)
        out, err = output.communicate()
        rc = output.returncode
        if rc != 0:
            raise RuntimeError('%s returned non-zero error code %s' % (cmd_printable, rc))
        output_data_file.close()


class DBMSTaskException(Exception):
    """
    Exception thrown by DBMSTask subclasses.
    """
    pass
