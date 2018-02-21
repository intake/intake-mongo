import os
import pytest

import intake_mongo

from intake import catalog
from . import util

TEST_DATA_DIR = 'tests'
TEST_DATA_CSV = [
    ('collection1', 'simple_collection.csv')
]

TEST_DATABASE = 'intake-test-database'


@pytest.fixture(scope='module')
def engine_uri():
    from .util import start_mongo, stop_mongo
    import pymongo
    import pandas as pd

    local_port = start_mongo()

    uri = 'mongodb://localhost:{}/test_db'.format(local_port)
    client = pymongo.MongoClient(uri)
    db = client[TEST_DATABASE]
    for collection_name, data_fname in TEST_DATA_CSV:
        data_fpath = os.path.join(TEST_DATA_DIR, data_fname)
        df = pd.read_csv(data_fpath)
        db[collection_name].insert_many(df.to_dict('records'))
        cursor = db[collection_name].find()
        print(pd.DataFrame(list(cursor)))

    try:
        yield uri
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
    data = plugin.open(engine_uri, dataset)
    assert data.container == 'dataframe'
    assert data.description == None
    util.verify_datasource_interface(data)
