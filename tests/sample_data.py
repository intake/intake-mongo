

import os
import pandas

TEST_DATA_DIR = 'test-data'
TEST_DATA_CSV = [
    ('collection1', 'simple_collection.csv')
]

_DATAFRAMES = {}


def _read_data():
    base_path = os.path.join(os.path.dirname(__file__), TEST_DATA_DIR)

    for name, filename in TEST_DATA_CSV:
        full_path = os.path.join(base_path, filename)
        df = pandas.read_csv(full_path)
        _DATAFRAMES[name] = df


_read_data()


def list_datasets():
    return _DATAFRAMES.keys()


def get_dataset(name):
    try:
        return _DATAFRAMES[name]
    except KeyError:
        return None


def get_dataset_fields(name):
    try:
        return _DATAFRAMES[name].columns.tolist()
    except KeyError:
        return None
