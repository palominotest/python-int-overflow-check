#!/usr/bin/env python
"""
Check for values in integer-type columns that had reached near maximum value.
"""

import argparse
import datetime
import logging
import logging.config
import pprint
import Queue
import threading
import string
import sys
import time

import MySQLdb

STATUS_OK = 0
STATUS_WARNING = 1
STATUS_CRITICAL = 2
STATUS_UNKNOWN = 3

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class NullHandler(logging.Handler):
    """
    This handler does nothing. It's intended to be used to avoid the
    "No handlers could be found for logger XXX" one-off warning. This is
    important for library code, which may contain code to log events. If a user
    of the library does not configure logging, the one-off warning might be
    produced; to avoid this, the library developer simply needs to instantiate
    a NullHandler and add it to the top-level logger of the library module or
    package.
    """

    def handle(self, record):
        """
        Handle a record. Does nothing in this class, but in other
        handlers it typically filters and then emits the record in a
        thread-safe way.
        """
        pass

    def emit(self, record):
        """
        Emit a record. This does nothing and shouldn't be called during normal
        processing, unless you redefine :meth:`~logutils.NullHandler.handle`.
        """
        pass

    def createLock(self):
        """
        Since this handler does nothing, it has no underlying I/O to protect
        against multi-threaded access, so this method returns `None`.
        """
        self.lock = None


log.addHandler(NullHandler())


def get_status_name(status):
    status_names = ['OK', 'WARN', 'CRIT', 'UNKNOWN']
    return status_names[status]


class Error(Exception):
    pass


def process_command_line(argv=None):
    """Returns parsed command line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            'Check for values in integer-type columns that had reached near '
            'maximum value.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        fromfile_prefix_chars='@'
    )

    parser.add_argument(
        '-H', '--hostname', help='Name of host to connect to.')

    parser.add_argument(
        '-P', '--port', type=int, default=3306,
        help='The port to be used when connecting to host.')

    parser.add_argument(
        '-u', '--user',
        help='The username to be used when connecting to host.')

    parser.add_argument(
        '-p', '--password',
        help='The password to be used when connecting to host.')

    parser.add_argument(
        '-d', '--use-dbs',
        help='A comma-separated list of db names to be inspected.')

    parser.add_argument(
        '-i', '--ignore-dbs',
        help='A comma-separated list of db names to be ignored.')

    parser.add_argument(
        '-T', '--threads', type=int, default=2,
        help='Number of threads to spawn.')

    parser.add_argument(
        '-e', '--exclude-columns',
        help=(
            'Specify columns to exclude in the following format: '
            'schema1.table1=col1,col2,colN;schemaN.tableN=colN;...'))

    parser.add_argument(
        '--row-count-max-ratio', default=50, type=float,
        help=(
            'If table row count ratio is less than this value, '
            'columns for this table are excluded from display.')
    )

    parser.add_argument(
        '--display-row-count-max-ratio-columns', action='store_true',
        help=(
            'In separate section, display columns containing high values '
            'compared to maximum for the column datatype, but row count '
            'ratio is less than the value of --row-count-max-ratio.')
    )

    parser.add_argument(
        '--results-host', help='Results database hostname.'
    )

    parser.add_argument(
        '--results-database', help='Results database name.'
    )

    parser.add_argument(
        '--results-user', help='Results database username.'
    )

    parser.add_argument(
        '--results-password', help='Results database password.'
    )

    parser.add_argument(
        '--results-port', type=int, help='Results database port.'
    )

    parser.add_argument(
        '--scan-all-columns', action='store_true',
        help='All columns are searched.'
    )

    parser.add_argument(
        '--secondary-keys', action='store_true',
        help='Secondary keys are also searched.'
    )

    parser.add_argument(
        '-w', '--warning', type=float, default=100.0,
        help='Warning threshold.'
    )

    parser.add_argument(
        '-c', '--critical', type=float, default=100.0,
        help='Critical threshold.'
    )

    parser.add_argument(
        '-L', '--logging-config', default=None,
        help='Logging configuration file.'
    )

    return parser.parse_args(argv)


def build_connection_options(parsed_args):
    """Returns MySQL connection options."""

    options = {}
    if parsed_args.hostname:
        options['host'] = parsed_args.hostname
    options['port'] = parsed_args.port
    if parsed_args.user:
        options['user'] = parsed_args.user
    if parsed_args.password:
        options['passwd'] = parsed_args.password
    return options


def fetchall(conn, query, args=None):
    """Executes query and returns all rows."""
    cur = conn.cursor()
    try:
        cur.execute(query, args)
        rows = cur.fetchall()
    finally:
        cur.close()
    return rows


def fetchone(conn, query, args=None):
    """Executes query and returns a single row."""
    cur = conn.cursor()
    try:
        cur.execute(query, args)
        row = cur.fetchone()
    finally:
        cur.close()
    return row


class TableProcessor(threading.Thread):
    """Worker thread for processing a table."""

    def __init__(self, schema_tables, parsed_args, results, *args, **kwargs):
        self.schema_tables = schema_tables
        self.parsed_args = parsed_args
        self.results = results

        super(TableProcessor, self).__init__(*args, **kwargs)
        self.daemon = True
        self.stop_event = threading.Event()

    def process_max_int(
            self, max_int, schema, table, column_name, column_type,
            row_count):
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

        critical_threshold = self.parsed_args.critical
        warning_threshold = self.parsed_args.warning

        row_count_max_ratio = self.parsed_args.row_count_max_ratio
        display_row_count_max_ratio_columns = (
            self.parsed_args.display_row_count_max_ratio_columns)

        log.debug(
            '[%s] overflow_percentage=%s, row_count_ratio=%s',
            self.getName(), overflow_percentage, row_count_ratio)

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
                log.debug(
                    '[%s] critical_column: \n%s',
                    self.getName(), pprint.pformat(critical_column))
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
                log.debug(
                    '[%s] investigate_column: \n%s',
                    self.getName(), pprint.pformat(investigate_column))

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
                log.debug(
                    '[%s] warning_colun: \n%s',
                    self.getName(), pprint.pformat(warning_column))
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
                log.debug(
                    '[%s] investigate_column: \n%s',
                    self.getName(), pprint.pformat(investigate_column))

    def run(self):
        log.debug('Thread [%s] started.', self.getName())
        try:
            while not self.stop_event.isSet():
                try:
                    schema_table = self.schema_tables.get(False, 5)
                    try:
                        schema = schema_table['schema']
                        table = schema_table['table']
                        columns = schema_table['columns']
                        row_count = schema_table['row_count']

                        log.debug(
                            "[%s] Processing '%s.%s'...",
                            self.getName(), schema, table)

                        conn = MySQLdb.connect(
                            **build_connection_options(self.parsed_args))
                        try:
                            for column in columns:
                                column_name = column['column_name']
                                column_type = column['column_type']

                                # Retrieve max value of integer
                                select_max = """
                                    SELECT MAX(`%s`) from `%s`.`%s`
                                    """ % (column_name, schema, table)

                                log.debug(
                                    '[%s] Query: %s',
                                    self.getName(), select_max)

                                row = fetchone(conn, select_max)
                                max_int = 0
                                if row:
                                    max_int = row[0]

                                log.debug(
                                    '[%s] max_int: %s',
                                    self.getName(), max_int)

                                self.process_max_int(
                                    max_int, schema, table, column_name,
                                    column_type, row_count)
                        finally:
                            conn.close()
                    finally:
                        # ensure that this is called so that the main thread
                        # will not wait forever

                        # on python 2.4, task_done does not exists
                        #self.schema_tables.task_done()
                        time.sleep(0)
                except Queue.Empty:
                    break

                except Exception, e:
                    error = 'ERROR %s: %s' % (type(e), e)
                    log.exception('[%s] %s', self.getName(), error)
                    self.results.put(dict(error=error))
                    sys.stderr.write('%s\n' % error)

        except Exception:
            # Queue method calls may throw exceptions when
            # interpreter is shutting down,
            # just ignore them
            log.exception('[%s] Exception.', self.getName())

        log.debug('Thread [%s] ended.', self.getName())


def create_exclude_columns_dict(s):
    """Convert string of format 'schema.table=col1,colN;...' to dict."""
    d = {}
    items = s.split(';')
    for item in items:
        schema_table, columns = item.split('=')
        column_list = columns.split(',')
        d[schema_table] = column_list
    return d


def get_schema_tables(parsed_args):
    query = """
        SELECT
            c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_TYPE,
            t.TABLE_ROWS, c.COLUMN_KEY, s.SEQ_IN_INDEX
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
        LEFT JOIN INFORMATION_SCHEMA.STATISTICS s
        ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME
        WHERE c.COLUMN_TYPE LIKE '%int%'
    """

    if parsed_args.use_dbs:
        # set comma separated schema names enclosed in single-quotes
        db_list = parsed_args.use_dbs.split(',')
        use_dbs = ','.join("'%s'" % db.strip() for db in db_list)
        if use_dbs:
            query += """
                AND c.TABLE_SCHEMA IN (%s)
                """ % (use_dbs,)

    if parsed_args.ignore_dbs:
        # set comma separated schema names enclosed in single-quotes
        db_list = parsed_args.ignore_dbs.split(',')
        ignore_dbs = ','.join("'%s'" % db.strip() for db in db_list)
        if ignore_dbs:
            query += """
                AND c.TABLE_SCHEMA NOT IN (%s)
                """ % (ignore_dbs,)

    conn = MySQLdb.connect(**build_connection_options(parsed_args))
    try:
        log.debug('%s', query)
        rows = fetchall(conn, query)
        log.debug('len(rows)=%s', len(rows))
        log.debug(pprint.pformat(rows))

        exclude_columns = None
        if parsed_args.exclude_columns:
            exclude_columns_str = parsed_args.exclude_columns.strip(
                string.whitespace + ';')
            if exclude_columns_str:
                exclude_columns = create_exclude_columns_dict(
                    exclude_columns_str)

        schema_tables = {}
        added_columns = []

        for row in rows:
            schema = row[0]
            table = row[1]
            column = row[2]
            column_type = row[3]
            row_count = row[4]
            column_key = row[5]
            if column_key is not None:
                column_key = column_key.strip().lower()
            seq_in_index = row[6]

            scan_secondary_keys = parsed_args.secondary_keys
            scan_all_columns = parsed_args.scan_all_columns

            schema_table = '%s.%s' % (schema, table)
            if (
                    exclude_columns and
                    schema_table in exclude_columns
                    and column in exclude_columns[schema_table]
                    ):
                # this column is excluded

                log.debug(
                    'Excluded column: %s.%s.%s',
                    schema, table, column)

                pass
            else:
                include_column = False

                if column_key and column_key == 'pri':
                    # always include primary keys
                    include_column = True

                if scan_secondary_keys:
                    if (
                            column_key and column_key != 'pri' and
                            seq_in_index and seq_in_index == 1):
                        include_column = True

                if scan_all_columns:
                    include_column = True

                if include_column:
                    column_to_add = '%s.%s.%s' % (schema, table, column)
                    if column_to_add in added_columns:
                        # prevent duplicates
                        include_column = False
                    else:
                        added_columns.append(column_to_add)

                if include_column:
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

def run_check(parsed_args):
    """Runs check."""

    status = STATUS_OK
    try:

        hostname = ''
        if parsed_args.hostname:
            hostname = parsed_args.hostname

        results_db_conn_opts = {}
        if parsed_args.results_host:
            results_db_conn_opts['host'] = parsed_args.results_host
        if parsed_args.results_port:
            results_db_conn_opts['port'] = parsed_args.results_port
        if parsed_args.results_user:
            results_db_conn_opts['user'] = parsed_args.results_user
        if parsed_args.results_password:
            results_db_conn_opts['passwd'] = parsed_args.results_password
        if parsed_args.results_database:
            results_db_conn_opts['db'] = parsed_args.results_database

        if results_db_conn_opts:
            if (
                    not ('db' in results_db_conn_opts and
                    results_db_conn_opts['db'])):
                raise Error('results_database is required.')

        log.debug(
            'Check started with the following options:\n%s', parsed_args)

        schema_tables = get_schema_tables(parsed_args)

        log.debug('Schema tables:\n%s', pprint.pformat(schema_tables))

        q = Queue.Queue()
        for v in schema_tables.itervalues():
            q.put(v)

        threads = parsed_args.threads
        results = Queue.Queue()
        thread_list = []
        for n in range(threads):
            thread = TableProcessor(
                schema_tables=q,
                parsed_args=parsed_args,
                results=results)
            thread.name = 'Thread #%d' % (n,)
            thread.daemon = True
            thread.start()
            thread_list.append(thread)

        # wait for all threads to finish
        log.debug('Waiting for all threads to finish running.')
        while True:
            all_dead = True
            for thread in thread_list:
                if thread.isAlive():
                    all_dead = False
                    break
            if all_dead:
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

            except Queue.Empty, e:
                break

        log.debug(
            'Critical columns:\n%s\n\nWarning columns:\n%s',
            pprint.pformat(critical_columns),
            pprint.pformat(warning_columns))

        if len(critical_columns) > 0:
            columns = sorted(critical_columns) + sorted(warning_columns)
            status = STATUS_CRITICAL
        elif len(warning_columns) > 0:
            columns = warning_columns
            status = STATUS_WARNING
        else:
            status = STATUS_OK

        msg = ''
        if status != STATUS_OK:
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
            if results_db_conn_opts:
                conn = MySQLdb.connect(**results_db_conn_opts)
                cursor = conn.cursor()
                try:
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
                finally:
                    cursor.close()

        row_count_max_ratio = parsed_args.row_count_max_ratio
        if investigate_columns:
            log.debug(
                'Investigate columns:\n%s',
                pprint.pformat(investigate_columns))

            if msg:
                msg += '\n'
            msg += (
                (
                    '\nColumns containing high values compared to maximum '
                    'for the column datatype, but number of rows is less '
                    'than %s%% of maximum for the column type:\n' % (
                        row_count_max_ratio,)) +
                (
                    '\n'.join('%s.%s\t%s\t%s\t%s\t%.2f%%' % (
                        col.get('schema'),
                        col.get('table'),
                        col.get('column_name'),
                        col.get('column_type'),
                        col.get('max_value'),
                        col.get('overflow_percentage'))
                        for col in investigate_columns))
            )

            ##############################################################
            # store investigate columns in db
            ##############################################################
            if results_db_conn_opts:
                conn = MySQLdb.connect(**results_db_conn_opts)
                cursor = conn.cursor()
                try:
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
                finally:
                    cursor.close()


        #print ('status: %s\n\nmsg:\n%s' % (status, msg))
        print '%s:%s' % (get_status_name(status), msg)
        return status
    except Exception, e:
        error = 'ERROR %s: %s' % (type(e), e)
        log.exception(error)
        sys.stderr.write(error)
        return STATUS_UNKNOWN


def main(argv=None):
    if not argv:
        argv = sys.argv[1:]
    parsed_args = process_command_line(argv)
    if parsed_args.logging_config:
        logging.config.fileConfig(parsed_args.logging_config)
    status = run_check(parsed_args)
    return status


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
