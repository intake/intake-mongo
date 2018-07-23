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


def test_simple_read(engine):
    s = intake_mongo.MongoDBSource(engine, DB, COLL)
    info = s.discover()
    assert info['npartitions'] == 1
    out = s.read()
    assert out == data
    s.close()
    assert s.collection is None


def test_read_kwargs(engine):
    s = intake_mongo.MongoDBSource(
        engine, DB, COLL, _id=False,
        find_kwargs={'projection': ['value']})
    data2 = [{'value': c['value']} for c in data]
    assert s.read() == data2
    s = intake_mongo.MongoDBSource(
        engine, DB, COLL, _id=False,
        find_kwargs={'projection': ['value'],
                     'sort': [('value', pymongo.ASCENDING)],
                     'limit': 10})
    assert s.read() == list(sorted(data2, key=lambda x: x['value']))[:10]
