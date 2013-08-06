Python Integer Overflow MySQL Check(Check Max Value)
================================

*Purpose: Check for max values in all columns of integer types that have reached N pct of the maximum value for that type of integer.*

*Notes: This is a translation of https://github.com/palominodb/palominodb-priv/tree/master/tools/mysql/int-overflow-check*

Install Requirements
-------------------------------

Install MySQL-python required packages:

For Ubuntu:
```
sudo apt-get install python-dev libmysqlclient-dev
```

It is recommended that virtual environment is used when using the tool rather than updating system packages.

Install virtualenv if you don't have it yet:

For Ubuntu:
```
sudo apt-get install python-virtualenv
```

Create virtual environment:
```
$ virtualenv --no-site-packages ~/myenv
```

Activate virtual environment and install requirements:
```
$ source ~/myenv/bin/activate
(myenv)$ pip install -r requirements.txt
```
At this point you can now run pdb_check_maxvalue.py script.

Deactivate virtual environment when done working with it:
```
(myenv)$ deactivate
```

For CentOS (tested on CentOS 5 and 6):
```
yum install mysql-devel
yum groupinstall "Development tools"
```
If you already have a version of python >2.7 installed somewhere, you can skip this step.  Install python version 2.7:

```
cd /tmp
wget http://python.org/ftp/python/2.7.3/Python-2.7.3.tar.bz2
tar xf Python-2.7.3.tar.bz2
cd Python-2.7.3
./configure --prefix=/usr/local
make
make altinstall
```
You may need sudo for "make altinstall" depending on what user you are running it as.

See if you have python-virtualenv available in your setup:
```
yum search python-virtualenv
```
If you do, run
```
yum install python-virtualenv
```
Otherwise, you may need to add the EPEL repo as shown here: http://www.rackspace.com/knowledge_center/article/installing-rhel-epel-repo-on-centos-5x-or-6x

Which is:

For CentOS 5:
```
wget http://dl.fedoraproject.org/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm
wget http://rpms.famillecollet.com/enterprise/remi-release-5.rpm
sudo rpm -Uvh remi-release-5*.rpm epel-release-5*.rpm
```

For CentOS 6:
```
wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
wget http://rpms.famillecollet.com/enterprise/remi-release-6.rpm
sudo rpm -Uvh remi-release-6*.rpm epel-release-6*.rpm
```
Then edit /etc/yum.repos.d/remi.repo and set enabled=1 in the [remi] section.
You should then be able to yum install python-virtualenv

Install your virtual environment:

```
virtualenv -p /usr/local/bin/python2.7 --no-site-packages ~/venv/myenv
```

Running the Script
------------------

To run a pdb_check_maxvalue.py in a virtual environment, you can either:

    * Activate virtual environment and run script with Python:

        $ source ~/myenv/bin/activate
        (myenv)$ python pdb_check_maxvalue.py <args>
        (myenv)$ deactivate
        $

        or if pdb_check_maxvalue.py's first line is: #!/usr/bin/env python

        $ source ~/myenv/bin/activate
        (myenv)$ ./pdb_check_maxvalue.py <args>
        (myenv)$ deactivate
        $

    * Run using full path of virtual environment's version of Python:

        $ ~/myenv/bin/python pdb_check_maxvalue.py <args>


Usage
-----

Usage: pdb_check_maxvalue.py [-h] [-H HOSTNAME] [-P PORT] [-u USER]
                             [-p PASSWORD] [-d USE_DBS] [-i IGNORE_DBS]
                             [-T THREADS] [-e EXCLUDE_COLUMNS]
                             [--row-count-max-ratio ROW_COUNT_MAX_RATIO]
                             [--display-row-count-max-ratio-columns]
                             [--results-host RESULTS_HOST]
                             [--results-database RESULTS_DATABASE]
                             [--results-user RESULTS_USER]
                             [--results-password RESULTS_PASSWORD]
                             [--results-port RESULTS_PORT]
                             [--scan-all-columns] [--secondary-keys]
                             [-w WARNING] [-c CRITICAL] [-L LOGGING_CONFIG]

Check for values in integer-type columns that had reached near maximum value.

Sample Usage:
  `pdb_check_maxvalue.py -u root -p password -d db1,db2,db3 --critical 75 --warning 50`

To read options from file:
  `pdb_check_maxvalue.py @args.sample.txt`

```

optional arguments:
  -h, --help            show this help message and exit
  -H HOSTNAME, --hostname HOSTNAME
                        Name of host to connect to. (default: None)
  -P PORT, --port PORT  The port to be used when connecting to host. (default:
                        3306)
  -u USER, --user USER  The username to be used when connecting to host.
                        (default: None)
  -p PASSWORD, --password PASSWORD
                        The password to be used when connecting to host.
                        (default: None)
  -d USE_DBS, --use-dbs USE_DBS
                        A comma-separated list of db names to be inspected.
                        (default: None)
  -i IGNORE_DBS, --ignore-dbs IGNORE_DBS
                        A comma-separated list of db names to be ignored.
                        (default: None)
  -T THREADS, --threads THREADS
                        Number of threads to spawn. (default: 2)
  -e EXCLUDE_COLUMNS, --exclude-columns EXCLUDE_COLUMNS
                        Specify columns to exclude in the following format:
                        schema1.table1=col1,col2,colN;schemaN.tableN=colN;...
                        (default: None)
  --row-count-max-ratio ROW_COUNT_MAX_RATIO
                        If table row count ratio is less than this value,
                        columns for this table are excluded from display.
                        (default: 50)
  --display-row-count-max-ratio-columns
                        In separate section, display columns containing high
                        values compared to maximum for the column datatype,
                        but row count ratio is less than the value of --row-
                        count-max-ratio. (default: False)
  --results-host RESULTS_HOST
                        Results database hostname. (default: None)
  --results-database RESULTS_DATABASE
                        Results database name. (default: None)
  --results-user RESULTS_USER
                        Results database username. (default: None)
  --results-password RESULTS_PASSWORD
                        Results database password. (default: None)
  --results-port RESULTS_PORT
                        Results database port. (default: None)
  --scan-all-columns    All columns are searched. (default: False)
  --secondary-keys      Secondary keys are also searched. (default: False)
  -w WARNING, --warning WARNING
                        Warning threshold. (default: 100.0)
  -c CRITICAL, --critical CRITICAL
                        Critical threshold. (default: 100.0)
  -L LOGGING_CONFIG, --logging-config LOGGING_CONFIG
                        Logging configuration file. (default: None)
```

  *When not in use, the Warning and Critical parameters are set to 100.*

To be able to store results in a database, create a table on the target database that will hold the results using the following statement:
```
CREATE TABLE `int_overflow_check_results` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hostname` varchar(255) DEFAULT NULL,
  `dbname` varchar(255) DEFAULT NULL,
  `table_name` varchar(255) DEFAULT NULL,
  `column_name` varchar(255) DEFAULT NULL,
  `max_size` bigint(20) unsigned DEFAULT NULL,
  `percentage` float DEFAULT NULL,
  `reason` text,
  `timestamp` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8
```


Testing
-------------------------------
*Make sure that the default user has CREATE grant*

In the script directory,
`python -m unittest tests.test`


Logging
-------

Logging can be configured only via configuration file.
To quickly enable logging, add the following in your configuration file and
pass the name of the configuration file via -L option:
```
[loggers]
keys=root,__main__

[handlers]
keys=new_file

[formatters]
keys=simple

[formatter_simple]
format=[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s

[handler_new_file]
formatter=simple
level=DEBUG
class=FileHandler
args=('debug.log', 'w', 'utf_8')

[logger_root]
level=NOTSET
handlers=new_file

[logger___main__]
level=DEBUG
handlers=new_file
propagate=0
qualname=__main__
```

There are many combinations of formatters and handlers.
If you need to customize the settings further, the following link will help you understand the details about the configuration file format:
http://docs.python.org/release/2.4.4/lib/logging-config-fileformat.html
