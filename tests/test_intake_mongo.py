import os
import pytest
import numpy

import intake_mongo

from intake import catalog
from . import util
#import util


TEST_DATA_DIR = 'tests'
TEST_DATA_CSV = [
    ('collection1', 'simple_collection.csv')
]

TEST_DATABASE = 'intake-test-database'

TEST_DATA_FIELDS = {}
TEST_DATA_DATAFRAMES = {}

def _fill_database(database):
    import pandas as pd

    for collection_name, data_fname in TEST_DATA_CSV:
        data_fpath = os.path.join(TEST_DATA_DIR, data_fname)
        df = pd.read_csv(data_fpath)
        database[collection_name].insert_many(df.to_dict('records'))
        TEST_DATA_DATAFRAMES[collection_name] = df
        TEST_DATA_FIELDS[collection_name] = df.columns.tolist()


@pytest.fixture(scope='module')
def engine_uri():
    from .util import start_mongo, stop_mongo
    import pymongo

    local_port = start_mongo()

    uri = 'mongodb://localhost:{}'.format(local_port)
    client = pymongo.MongoClient(uri)
    _fill_database(client[TEST_DATABASE])
    try:
        yield uri + '/' + TEST_DATABASE
    finally:
        stop_mongo()


def test_mongo_plugin():
    plugin = intake_mongo.Plugin()
    
    util.verify_plugin_interface(plugin)
    
    # The mongo plugin should use a dataframe interface
    assert plugin.container == 'dataframe'


@pytest.mark.parametrize('dataset, _', TEST_DATA_CSV)
def test_open(engine_uri, dataset, _):
    plugin = intake_mongo.Plugin()
    data = plugin.open(engine_uri, dataset, TEST_DATA_FIELDS[dataset])
    assert data.container == 'dataframe'
    assert data.description == None
    util.verify_datasource_interface(data)


@pytest.mark.parametrize('dataset, _', TEST_DATA_CSV)
def test_discover(engine_uri, dataset, _):
    fields = TEST_DATA_FIELDS[dataset]
    expected_df = TEST_DATA_DATAFRAMES[dataset]
    expected_dtype = _build_expected_dtype(expected_df)

    #print(engine_uri, dataset, fields)
    plugin = intake_mongo.Plugin()
    data = plugin.open(engine_uri, dataset, fields)
    info = data.discover()
    util.verify_open_datasource_interface(data)
    #print('fields:', fields)
    #print('got: ', data.dtype)
    #print('expected:', expected_dtype)

    # FAILS due to mongoadapter prefering "unsigned" types vs "signed"
    # preferred by pandas' read_csv used to read in sample data.
    assert info['dtype'] == expected_dtype
    # mongo plugin does not known the number of tuples before reading
    # (unless "count" is used, which is actually exposed by mongoadapter
    # but it is reportedly slow)
    assert info['shape'] == (None,)
    # mongo plugin yields all in one partition
    assert info['npartitions'] == 1
