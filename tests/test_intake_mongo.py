import os
import pytest
import numpy

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
