import pytest
import pymongo

import intake
import intake_mongo

from . import util  # interface verification
from .mongo_test_instance import start_mongo, URI, stop_docker, data, DB, COLL


@pytest.fixture(scope='module')
def engine():
    cid = start_mongo()
    try:
        yield URI
    finally:
        stop_docker(cid=cid)


def test_mongo_plugin():
    plugin = intake_mongo.Plugin()

    util.verify_plugin_interface(plugin)
    assert plugin.container == 'python'


def test_simple_read(engine):
    plugin = intake_mongo.Plugin()
    s = plugin.open(engine, DB, COLL)
    info = s.discover()
    assert info['npartitions'] == 1
    out = s.read()
    assert out == data
    s.close()
    assert s.collection is None


def test_read_kwargs(engine):
    s = intake.open_mongo(engine, DB, COLL, _id=False,
                          find_kwargs={'projection': ['value']})
    data2 = [{'value': c['value']} for c in data]
    assert s.read() == data2
    s = intake.open_mongo(engine, DB, COLL, _id=False,
                          find_kwargs={'projection': ['value'],
                                       'sort': [('value', pymongo.ASCENDING)],
                                       'limit': 10})
    assert s.read() == list(sorted(data2, key=lambda x: x['value']))[:10]
