import os
import pytest
import numpy

import intake_mongo

from intake import catalog
from . import sample_data
from . import util # interface verification


@pytest.fixture(scope='module')
def engine_uri():
    from .mongo_test_instance import testing_instance, interactive_instance

    instance_fn = (interactive_instance if os.getenv('REUSE_MONGO')
                   else testing_instance)
    with instance_fn() as mongo_uri:
        yield mongo_uri


def test_mongo_plugin():
    plugin = intake_mongo.Plugin()
    
    util.verify_plugin_interface(plugin)
    
    # The mongo plugin should use a dataframe interface
    assert plugin.container == 'dataframe'


@pytest.mark.parametrize('dataset', sample_data.list_datasets())
def test_open(engine_uri, dataset):
    plugin = intake_mongo.Plugin()
    data = plugin.open(engine_uri, dataset, sample_data.get_dataset_fields(dataset))
    assert data.container == 'dataframe'
    util.verify_datasource_interface(data)


def _fuzzy_check_base_type(lhdt, rhdt):
    """check that the dtypes (lhdt, rhdt) are equivalent, considering
    all integers regardless of size and signedness equal"""
    return (lhdt == rhdt or
            (numpy.issubdtype(lhdt, numpy.integer) and
             numpy.issubdtype(rhdt, numpy.integer)))


@pytest.mark.parametrize('dataset', sample_data.list_datasets())
def test_discover(engine_uri, dataset):
    fields = sample_data.get_dataset_fields(dataset)
    expected_df = sample_data.get_dataset(dataset)

    plugin = intake_mongo.Plugin()
    data = plugin.open(engine_uri, dataset, fields)
    info = data.discover()
    util.verify_open_datasource_interface(data)

    assert expected_df.dtypes.to_dict() == expected_df.dtypes.to_dict()
    # mongo plugin does not known the number of tuples before reading
    # (unless "count" is used, which is actually exposed by mongoadapter
    # but it is reportedly slow)
    assert info['shape'] == (None, len(expected_df.dtypes))
    # mongo plugin yields all in one partition
    assert info['npartitions'] == 1
