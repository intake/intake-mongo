#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-mongo',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='MongoDB plugin for Intake',
    url='https://github.com/ContinuumIO/intake-mongo',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'mongo = intake_mongo.intake_mongo:MongoDBSource',
        ]},
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
