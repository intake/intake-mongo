import pandas as pd


def _check_attribute(obj, attr, typ, allow_none=False):
    """Check that a given attribute attr exists in the object obj and
    is of type typ. Optionally, allow the attribute to be None.
    """
    if not hasattr(obj, attr):
        return False

    val = getattr(obj, attr)
    return isinstance(val, typ) or (allow_none and val is None)


def _check_method(obj, meth):
    """Check that an object obj has a method meth."""
    if not hasattr(obj, meth):
        return False

    return callable(getattr(obj, meth))


def verify_plugin_interface(plugin):
    """Assert types of plugin attributes."""

    # check attributes
    assert isinstance(plugin.partition_access, bool)
    assert _check_attribute(plugin, 'name', str)
    assert _check_attribute(plugin, 'version', str)
    assert _check_attribute(plugin, 'container', str)
    assert plugin.container in ('dataframe', 'ndarray', 'python')
    assert _check_attribute(plugin, 'partition_access', bool)

    # methods
    assert _check_method(plugin, 'open')


def verify_datasource_interface(source):
    """Assert presence of datasource attributes"""

    # attributes that need to be present always
    assert _check_attribute(source, 'container', str)

    # methods that need to be present always
    assert _check_method(source, 'discover')
    assert _check_method(source, 'read')
    assert _check_method(source, 'read_chunked')
    assert _check_method(source, 'read_partition')
    assert _check_method(source, 'to_dask')
    assert _check_method(source, 'close')


def verify_open_datasource_interface(source):
    """Verify that an *open* datasource adheres to the documented interface."""
    # it must adhere to the base datasource interface
    verify_datasource_interface(source)
    assert _check_attribute(source, 'metadata', dict)
    assert _check_attribute(source, 'datashape', str, allow_none=True)
    assert _check_attribute(source, 'dtype', pd.Series, allow_none=True)
    assert _check_attribute(source, 'shape', tuple, allow_none=True)
    assert source.shape is None or all(isinstance(x, int) or x is None
                                       for x in source.shape)
    assert _check_attribute(source, 'npartitions', int)
