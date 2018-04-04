#!/usr/bin/env python

from setuptools import setup

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-mongo',
    version='0.0.1',
    description='MongoDB plugin for Intake',
    url='https://github.com/ContinuumIO/intake-mongo',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    py_modules=['intake_mongo'],
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
