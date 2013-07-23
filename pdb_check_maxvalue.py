#!/usr/bin/env python
#
# Author: Rod Xavier Bondoc <RXavier@palominodb.com>
# Date: February 2013
# File: pdb_check_maxvalue.py
# Purpose: Check max values of integer columns and report columns which is over the threshold
#
# Notes:
#   - This is a translation of https://github.com/palominodb/palominodb-priv/tree/master/tools/mysql/int-overflow-check
#

import logging
import pprint
import Queue
import threading
import time

import MySQLdb
import datetime
import pynagios
from pynagios import Plugin, Response, make_option
import yaml

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

# use this in all your library's subpackages/submodules
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# use this just in your library's top-level package
log.addHandler(NullHandler())

pp = pprint.pprint


class Error(Exception):
    pass


def fetchall(conn, query, args=None):
    """Executes query and returns all rows."""
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(query, args)
        rows = cur.fetchall()
    finally:
        cur.close()
    return rows


def fetchone(conn, query, args=None):
    """Executes query and returns a single row."""
    row = None
    cur = conn.cursor()
    try:
        cur.execute(query, args)
        row = cur.fetchone()
    finally:
        cur.close()
    return row


class TableProcessor(threading.Thread):
    """Worker thread for processing a table."""
    def __init__(self, *args, **kwargs):
        self.schema_tables = kwargs.pop('schema_tables')
        self.merged_options = kwargs.pop('merged_options')
        self.results = kwargs.pop('results')
        super(TableProcessor, self).__init__(*args, **kwargs)
        self.daemon = True
        self.stop_event = threading.Event()

    def process_max_int(
            self, max_int, schema, table, column_name, column_type, row_count):
        # Initialize constants
        type_tinyint = 127.0
        type_smallint = 32767.0
        type_mediumint = 8388607.0
        type_int = 2147483647.0
        type_bigint = 9223372036854775807.0

        type_tinyint_us = 255.0
        type_smallint_us = 65535.0
        type_mediumint_us = 16777215.0
        type_int_us = 4294967295.0
        type_bigint_us = 18446744073709551615.0

        # Initialize dubious fields
        if max_int is None:
            max_int = 0

        # Check if type is signed or unsigned
        unsigned = False
        if 'unsigned' in column_type:
            unsigned = True

        int_type = column_type.split('(')[0]

        row_count_ratio = 0

        # Compute overflow percentage
        overflow_percentage = 0
        if not unsigned:
            if int_type == 'tinyint':
                overflow_percentage = (max_int / type_tinyint) * 100
                row_count_ratio = (row_count / type_tinyint) * 100
            elif int_type == 'smallint':
                overflow_percentage = (max_int / type_smallint) * 100
                row_count_ratio = (row_count / type_smallint) * 100
            elif int_type == 'mediumint':
                overflow_percentage = (max_int / type_mediumint) * 100
                row_count_ratio = (row_count / type_mediumint) * 100
            elif int_type == 'int':
                overflow_percentage = (max_int / type_int) * 100
                row_count_ratio = (row_count / type_int) * 100
            elif int_type == 'bigint':
                overflow_percentage = (max_int / type_bigint) * 100
                row_count_ratio = (row_count / type_bigint) * 100
        else:
            if int_type == 'tinyint':
                overflow_percentage = (max_int / type_tinyint_us) * 100
                row_count_ratio = (row_count / type_tinyint_us) * 100
            elif int_type == 'smallint':
                overflow_percentage = (max_int / type_smallint_us) * 100
                row_count_ratio = (row_count / type_smallint_us) * 100
            elif int_type == 'mediumint':
                overflow_percentage = (max_int / type_mediumint_us) * 100
                row_count_ratio = (row_count / type_mediumint_us) * 100
            elif int_type == 'int':
                overflow_percentage = (max_int / type_int_us) * 100
                row_count_ratio = (row_count / type_int_us) * 100
            elif int_type == 'bigint':
                overflow_percentage = (max_int / type_bigint_us) * 100
                row_count_ratio = (row_count / type_bigint_us) * 100



        critical_threshold = self.merged_options['critical']
        warning_threshold = self.merged_options['warning']

        row_count_max_ratio = self.merged_options.get('row_count_max_ratio')
        display_row_count_max_ratio_columns = self.merged_options.get('display_row_count_max_ratio_columns')

        log.debug('[%s] overflow_percentage=%s, row_count_ratio=%s' % (
            self.name, overflow_percentage, row_count_ratio))

        if overflow_percentage > critical_threshold:
            if row_count_ratio >= row_count_max_ratio:
                critical_column = {
                    'schema': schema,
                    'table': table,
                    'column_name': column_name,
                    'column_type': column_type,
                    'max_value': max_int,
                    'overflow_percentage': overflow_percentage,
                }
                self.results.put(dict(critical_column=critical_column))
                log.debug('[%s] critical_column: \n%s' % (
                    self.name, pprint.pformat(critical_column)))
            elif display_row_count_max_ratio_columns:
                investigate_column = dict(
                    schema=schema,
                    table=table,
                    column_name=column_name,
                    column_type=column_type,
                    max_value=max_int,
                    overflow_percentage=overflow_percentage,
                    row_count_ratio=row_count_ratio
                )
                self.results.put(dict(
                    investigate_column=investigate_column
                ))
                log.debug('[%s] investigate_column: \n%s' % (
                    self.name, pprint.pformat(investigate_column)))

        elif overflow_percentage > warning_threshold:
            if row_count_ratio >= row_count_max_ratio:
                warning_column = {
                    'schema': schema,
                    'table': table,
                    'column_name': column_name,
                    'column_type': column_type,
                    'max_value': max_int,
                    'overflow_percentage': overflow_percentage,
                }
                self.results.put(dict(warning_column=warning_column))
                log.debug('[%s] warning_colun: \n%s' % (
                    self.name, pprint.pformat(warning_column)))
            elif display_row_count_max_ratio_columns:
                investigate_column = dict(
                    schema=schema,
                    table=table,
                    colum_name=column_name,
                    column_type=column_type,
                    max_value=max_int,
                    overflow_percentage=overflow_percentage,
                    row_count_ratio=row_count_ratio
                )
                self.results.put(dict(
                    investigate_column=investigate_column
                ))
                log.debug('[%s] investigate_column: \n%s' % (
                    self.name, pprint.pformat(investigate_column)))

    def run(self):
        log.debug('Thread [%s] started.' % (self.name,))
        try:
            while not self.stop_event.is_set():
                try:
                    schema_table = self.schema_tables.get(False, 5)
                    try:
                        schema = schema_table['schema']
                        table = schema_table['table']
                        columns = schema_table['columns']
                        row_count = schema_table['row_count']

                        log.debug("[%s] Processing '%s.%s'..." % (
                            self.name, schema, table))

                        conn = create_connection(self.merged_options)
                        try:
                            for column in columns:
                                column_name = column['column_name']
                                column_type = column['column_type']

                                # Retrieve max value of integer
                                select_max = """
                                    SELECT MAX(`%s`) from `%s`.`%s`
                                    """ % (column_name, schema, table)

                                log.debug('[%s] Query: %s' % (self.name, select_max))

                                row = fetchone(conn, select_max)
                                max_int = 0
                                if row:
                                    max_int = row[0]

                                log.debug('[%s] max_int: %s' % (self.name, max_int))

                                self.process_max_int(
                                    max_int, schema, table, column_name,
                                    column_type, row_count)
                        finally:
                            conn.close()
                    finally:
                        # ensure that this is called so that the main thread
                        # will not wait forever
                        self.schema_tables.task_done()
                        time.sleep(0)
                except Queue.Empty:
                    break

                except Exception, e:
                    log.exception('[%s] Exception.' % (self.name,))
                    error = '%s: %s' % (type(e), e)
                    self.results.put(dict(error=error))


        except:
            # Queue method calls may throw exceptions when
            # interpreter is shutting down,
            # just ignore them
            log.exception('[%s] Exception.' % (self.name,))
            pass

        log.debug('Thread [%s] ended.' % (self.name,))

def create_connection(merged_options):
    """Returns mysql connection."""

    connection_options = {}
    if 'hostname' in merged_options and merged_options['hostname']:
        connection_options['host'] = merged_options['hostname']
    if 'port' in merged_options and merged_options['port']:
        connection_options['port'] = int(merged_options['port'])
    if 'user' in merged_options and merged_options['user']:
        connection_options['user'] = merged_options['user']
    if 'password' in merged_options and merged_options['password']:
        connection_options['passwd'] = merged_options['password']
    return MySQLdb.connect(**connection_options)


class CheckMaxValue(Plugin):
    """A nagios plugin for checking Integer Overflow"""

    port = make_option(
        '-P', '--port', dest='port', type='int', default=3306,
        help='The port to be used')

    user = make_option(
        '-u', '--user', dest='user', help='Database user')

    password = make_option(
        '-p', '--password', dest='password', help='Database password')

    use_dbs = make_option(
        '-d', '--use-dbs', dest='use_dbs',
        help='A comma-separated list of db names to be inspected')

    ignore_dbs = make_option(
        '-i', '--ignore-dbs', dest='ignore_dbs',
        help='A comma-separated list of db names to be ignored')

    config = make_option(
        '-C', '--config', dest='config', help='Configuration filename')

    threads = make_option(
        '-T', '--threads', dest='threads', type=int, default=2,
        help='Number of threads to spawn')

    exclude_columns = make_option(
        '-e', '--exclude-columns', dest='exclude_columns',
        help=(
            'Specify columns to exclude in the following format: '
            'schema1.table1=col1,col2,colN;schemaN.tableN=colN;...'))

    row_count_max_ratio = make_option(
        '--row-count-max-ratio',
        default=50, type=float,
        help='If table row count is less than this value, exclude this column from display.'
    )

    display_row_count_max_ratio_columns = make_option(
        '--display-row-count-max-ratio-columns',
        action='store_true',
        help='In separate section, display columns containing high values compared to maximum for the column datatype, but number of rows is less than the value of --row-count-max-ratio.'
    )


    results_host = make_option(
        '--results-host',
        default=None,
        help='Results database hostname.'
    )

    results_database = make_option(
        '--results-database',
        default=None,
        help='Results database name.'
    )

    results_user = make_option(
        '--results-user',
        default=None,
        help='Results database username.'
    )

    results_password = make_option(
        '--results-password',
        default=None,
        help='Results database password.'
    )

    results_port = make_option(
        '--results-port',
        default=None,
        help='Results database port.'
    )

    def get_options_from_config_file(self):
        """Returns options from YAML file."""
        if self.options.config:
            with open(self.options.config) as f:
                return yaml.load(f)
        else:
            return None

    def get_merged_options(self, additional_options):
        """Returns argument options merged with additional options."""
        options = {}
        if self.options.ignore_dbs:
            options['ignore_dbs'] = self.options.ignore_dbs
        if self.options.use_dbs:
            options['use_dbs'] = self.options.use_dbs
        if self.options.port:
            options['port'] = self.options.port
        if self.options.user:
            options['user'] = self.options.user
        if self.options.password:
            options['password'] = self.options.password
        if self.options.hostname:
            options['hostname'] = self.options.hostname
        if self.options.warning:
            options['warning'] = self.options.warning
        if self.options.critical:
            options['critical'] = self.options.critical
        if self.options.threads:
            options['threads'] = self.options.threads
        if self.options.exclude_columns:
            options['exclude_columns'] = self.options.exclude_columns
        if self.options.row_count_max_ratio:
            options['row_count_max_ratio'] = self.options.row_count_max_ratio
        if self.options.display_row_count_max_ratio_columns:
            options['display_row_count_max_ratio_columns'] = self.options.display_row_count_max_ratio_columns

        if self.options.results_host:
            options['results_host'] = self.options.results_host
        if self.options.results_database:
            options['results_database'] = self.options.results_database
        if self.options.results_user:
            options['results_user'] = self.options.results_user
        if self.options.results_password:
            options['results_password'] = self.options.results_password
        if self.options.results_port:
            options['results_port'] = self.options.results_port

        if additional_options:
            options.update(additional_options)
        return options

    def create_exclude_columns_dict(self, s):
        """Convert string of format 'schema.table=col1,colN;...' to dict."""
        d = {}
        items = s.split(';')
        for item in items:
            schema_table, columns = item.split('=')
            column_list = columns.split(',')
            d[schema_table] = column_list
        return d


    def merge_options(self):
        self.config_options = self.get_options_from_config_file()
        merged_options = self.get_merged_options(
            self.config_options)

        # Thresholds
        if self.config_options and 'critical' in self.config_options:
            critical = float(self.config_options['critical'])
        else:
            critical = (
                float(self.options.critical.__str__())
                if self.options.critical is not None else 100)
        if self.config_options and 'warning' in self.config_options:
            warning = float(self.config_options['warning'])
        else:
            warning = (
                float(self.options.warning.__str__())
                if self.options.warning is not None else 100)

        merged_options['critical'] = critical
        merged_options['warning'] = warning

        # fix string versions of ignore_dbs, use_dbs, exclude_columns
        if 'ignore_dbs' in merged_options:
            ignore_dbs = merged_options['ignore_dbs']
            if ignore_dbs and isinstance(ignore_dbs, basestring):
                # convert string to list
                ignore_dbs = ignore_dbs.strip()
                if ignore_dbs:
                    merged_options['ignore_dbs'] = ignore_dbs.split(',')
        if 'use_dbs' in merged_options:
            use_dbs = merged_options['use_dbs']
            if use_dbs and isinstance(use_dbs, basestring):
                # convert string to list
                use_dbs = use_dbs.strip()
                if use_dbs:
                    merged_options['use_dbs'] = use_dbs.split(',')
        if 'exclude_columns' in merged_options:
            exclude_columns = merged_options['exclude_columns']
            if exclude_columns and isinstance(exclude_columns, basestring):
                # convert string to dict
                exclude_columns = exclude_columns.strip('; ')
                if exclude_columns:
                    merged_options['exclude_columns'] = (
                        self.create_exclude_columns_dict(exclude_columns))

        self.merged_options = merged_options

    def get_schema_tables(self):
        merged_options = self.merged_options

        query = """
            SELECT
                c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_TYPE,
                t.TABLE_ROWS
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN INFORMATION_SCHEMA.TABLES t
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.COLUMN_TYPE LIKE '%int%'
        """

        if 'use_dbs' in merged_options:
            # set comma separated schema names enclosed in single-quotes
            use_dbs = ','.join(
                "'%s'" % (db,) for db in merged_options['use_dbs'])
            if use_dbs:
                query += """
                    AND c.TABLE_SCHEMA IN (%s)
                    """ % (use_dbs,)

        if 'ignore_dbs' in merged_options:
            # set comma separated schema names enclosed in single-quotes
            ignore_dbs = ','.join(
                "'%s'" % (db,) for db in merged_options['ignore_dbs'])
            if ignore_dbs:
                query += """
                    AND c.TABLE_SCHEMA NOT IN (%s)
                    """ % (ignore_dbs,)

        conn = create_connection(merged_options)
        try:
            log.debug('%s' % (query,))
            rows = fetchall(conn, query)
            log.debug('len(rows)=%s' % (len(rows),))

            if 'exclude_columns' in self.merged_options:
                exclude_columns = self.merged_options['exclude_columns']
            else:
                exclude_columns = None

            schema_tables = {}

            for row in rows:
                schema = row[0]
                table = row[1]
                column = row[2]
                column_type = row[3]
                row_count = row[4]

                schema_table = '%s.%s' % (schema, table)
                if (
                        exclude_columns and
                        schema_table in exclude_columns
                        and column in exclude_columns[schema_table]
                        ):
                    # this column is excluded

                    log.debug('Excluded column: %s.%s.%s' % (
                        schema, table, column))

                    pass
                else:
                    if schema_table in schema_tables:
                        schema_tables[schema_table]['columns'].append(
                            dict(
                                column_name=column,
                                column_type=column_type))
                    else:
                        schema_tables[schema_table] = dict(
                            schema=schema,
                            table=table,
                            row_count=row_count,
                            columns=[dict(
                                column_name=column,
                                column_type=column_type)])

            # end for
        finally:
            conn.close()

        return schema_tables

    def configure_logging(self):
        try:
            from logging.config import dictConfig
        except ImportError:
            from logutils.dictconfig import dictConfig

        if 'logging' in self.merged_options and self.merged_options['logging']:
            dictConfig(self.merged_options['logging'])

    def check(self):
        try:
            self.merge_options()
            self.configure_logging()

            merged_options = self.merged_options
            hostname = ''
            if 'hostname' in merged_options and merged_options['hostname']:
                hostname = merged_options['hostname']
            self.results_db_conn_opts = {}

            if 'results_host' in merged_options and merged_options['results_host']:
                self.results_db_conn_opts['host'] = merged_options['results_host']

            if 'results_port' in merged_options and merged_options['results_port']:
                self.results_db_conn_opts['port'] = merged_options['results_port']

            if 'results_user' in merged_options and merged_options['results_user']:
                self.results_db_conn_opts['user'] = merged_options['results_user']

            if 'results_password' in merged_options and merged_options['results_password']:
                self.results_db_conn_opts['passwd'] = merged_options['results_password']

            if 'results_database' in merged_options and merged_options['results_database']:
                self.results_db_conn_opts['db'] = merged_options['results_database']

            if self.results_db_conn_opts:
                if not ('db' in self.results_db_conn_opts and self.results_db_conn_opts['db']):
                    raise Error('results_database is required.')

            log.debug('Check started with the following options:\n%s' % (
                pprint.pformat(self.merged_options),))

            schema_tables = self.get_schema_tables()

            log.debug('Schema tables:\n%s' % (pprint.pformat(schema_tables),))

            q = Queue.Queue()
            for v in schema_tables.itervalues():
                q.put(v)

            threads = self.merged_options['threads']
            results = Queue.Queue()
            thread_list = []
            for n in range(threads):
                thread = TableProcessor(
                    schema_tables=q,
                    merged_options=self.merged_options,
                    results=results)
                thread.name = 'Thread #%d' % (n,)
                thread.daemon = True
                thread.start()
                thread_list.append(thread)

            # wait for all threads to finish
            log.debug('Waiting for all threads to finish running.')
            while True:
                dead = []
                for thread in thread_list:
                    dead.append(not thread.is_alive())
                if all(dead):
                    break
                time.sleep(0.01)
            log.debug('All threads finished.')

            critical_columns = []
            warning_columns = []
            errors = []
            investigate_columns = []
            while True:
                try:
                    result = results.get_nowait()

                    if 'critical_column' in result:
                        critical_columns.append(result['critical_column'])

                    if 'warning_column' in result:
                        warning_columns.append(result['warning_column'])

                    if 'error' in result:
                        errors.append(result['error'])

                    if 'investigate_column' in result:
                        investigate_columns.append(result['investigate_column'])

                    results.task_done()
                except Queue.Empty, e:
                    break

            log.info('Critical columns:\n%s\n\nWarning columns:\n%s' % (
                pprint.pformat(critical_columns),
                pprint.pformat(warning_columns)))

            if len(critical_columns) > 0:
                columns = sorted(critical_columns) + sorted(warning_columns)
                status = pynagios.CRITICAL
            elif len(warning_columns) > 0:
                columns = warning_columns
                status = pynagios.WARNING
            else:
                status = pynagios.OK

            msg = ''
            if status != pynagios.OK:
                msg = '\n'.join(
                    '%s.%s\t%s\t%s\t%s\t%.2f%%' % (
                        col.get('schema'),
                        col.get('table'),
                        col.get('column_name'),
                        col.get('column_type'),
                        col.get('max_value'),
                        col.get('overflow_percentage')) for col in columns)
                msg = '\n' + msg

                ##############################################################
                # store critical/warning columns in db
                ##############################################################
                if self.results_db_conn_opts:
                    conn = MySQLdb.connect(**self.results_db_conn_opts)
                    with conn as cursor:
                        sql = (
                            "INSERT INTO int_overflow_check_results("
                            "  hostname, dbname, table_name, column_name, "
                            "  max_size, percentage, reason, timestamp) "
                            "VALUE (%s, %s, %s, %s, %s, %s, %s, %s)")

                        for col in critical_columns:
                            cursor.execute(
                                sql,
                                (
                                    hostname, col.get('schema'),
                                    col.get('table'),
                                    col.get('column_name'),
                                    col.get('max_value'),
                                    col.get('overflow_percentage'),
                                    'critical', datetime.datetime.now()))

                        for col in warning_columns:
                            cursor.execute(
                                sql,
                                (
                                    hostname, col.get('schema'),
                                    col.get('table'),
                                    col.get('column_name'),
                                    col.get('max_value'),
                                    col.get('overflow_percentage'),
                                    'warning', datetime.datetime.now()))


            row_count_max_ratio = self.merged_options.get('row_count_max_ratio', 0)
            if investigate_columns:
                log.info('Investigate columns:\n%s' % (pprint.pformat(
                    investigate_columns,)))
                if msg:
                    msg += '\n'
                msg += (
                    ('\nColumns containing high values compared to maximum for the column datatype, but number of rows is less than %s%% of maximum for the column type:\n' % (row_count_max_ratio,)) +
                    ('\n'.join(
                        '%s.%s\t%s\t%s\t%s\t%.2f%%' % (
                            col.get('schema'),
                            col.get('table'),
                            col.get('column_name'),
                            col.get('column_type'),
                            col.get('max_value'),
                            col.get('overflow_percentage')) for col in investigate_columns))
                    )

                ##############################################################
                # store investigate columns in db
                ##############################################################
                if self.results_db_conn_opts:
                    conn = MySQLdb.connect(**self.results_db_conn_opts)
                    with conn as cursor:
                        sql = (
                            "INSERT INTO int_overflow_check_results("
                            "  hostname, dbname, table_name, column_name, "
                            "  max_size, percentage, reason, timestamp) "
                            "VALUE (%s, %s, %s, %s, %s, %s, %s, %s)")

                        for col in investigate_columns:
                            cursor.execute(
                                sql,
                                (
                                    hostname, col.get('schema'),
                                    col.get('table'),
                                    col.get('column_name'),
                                    col.get('max_value'),
                                    col.get('overflow_percentage'),
                                    'investigate', datetime.datetime.now()))


            log.info('status: %s\n\nmsg:\n%s' % (status, msg))

            self.exit_code = status.exit_code
            return Response(status, msg)
        except Exception, e:
            log.exception('Exception.')
            return Response(pynagios.UNKNOWN, 'ERROR: {0}'.format(e))


if __name__ == "__main__":
    # Instantiate the plugin, check it, and then exit
    CheckMaxValue().check().exit()
