#!/usr/bin/env python

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages
setup(
    name='int-overflow-check',
    version='0.1a.dev',
    packages=find_packages(exclude=['tests']),
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
