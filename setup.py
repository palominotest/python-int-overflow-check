#!/usr/bin/env python

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages
setup(
    name='int-overflow-check',
    version='0.1b.dev',
    author='Elmer Medez',
    author_email='oss@palominodb.com',
    packages=find_packages(exclude=['tests']),
    data_files=[('.',['docs/*'])],
    url="http://pypi.python.org/pypi/int-overflow-check",
    license='GPLv2',
    description='Check MySQL tables for potential integer overflows',
    install_requires=[
        'MySQL-python',
        'argparse',
    ],
    entry_points={
        'console_scripts': [
            'pdb_check_maxvalue = int_overflow_check.pdb_check_maxvalue:main',
        ]
    }
)
