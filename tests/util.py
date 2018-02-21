import subprocess
import shlex
import numpy



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

    #check attributes
    assert isinstance(plugin.partition_access, bool)
    assert _check_attribute(plugin, 'name', str)
    assert _check_attribute(plugin, 'version', str)
    assert _check_attribute(plugin, 'api_version', int)
    assert _check_attribute(plugin, 'container', str)
    assert plugin.container in ('dataframe', 'ndarray', 'python')
    assert _check_attribute(plugin, 'partition_access', bool)

    # methods
    assert _check_method(plugin, 'open')


def verify_datasource_interface(source):
    """Assert presence of datasource attributes"""

    # attributes that need to be present always
    assert _check_attribute(source, 'name', str)
    assert _check_attribute(source, 'container', str)
    assert _check_attribute(source, 'description', str, allow_none=True)
 
    assert _check_attribute(source, 'metadata', dict)
    assert _check_attribute(source, 'datashape', str)
    assert _check_attribute(source, 'dtype', str)
    assert _check_attribute(source, 'shape', tuple)
    assert all(isinstance(x, int) for x in source.shape)
    assert _check_attribute(source, 'npartitions', int)
    assert _check_attribute(source, 'partition_map', dict, allow_none=True)
    if source.partition_map is not None:
        assert all(isinstance(key, int) for key in source.partition_map.keys())
        assert all(isinstance(value, list) and
                   all(isinstance(hostname, str) for hostname in value)
                   for value in source.partition_map.values())
        
    # methods that need to be present always
    assert _check_method(source, 'discover')
    assert _check_method(source, 'read')
    assert _check_method(source, 'read_chunked')
    assert _check_method(source, 'read_partition')
    assert _check_method(source, 'to_dask')
    assert _check_method(source, 'close')
    assert _check_method(source, 'plot')


_MONGODB_INSTANCE_NAME = 'intake-mongodb-test'

def start_mongo():
    """Bring up a container running mongo.

    Returns the local port as a string.
    """

    print('Starting MongoDB server...')

    cmd = shlex.split('docker run --rm --name {} --publish 27017 '
                      'mongo:latest'.format(_MONGODB_INSTANCE_NAME))
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)

    while True:
        output_line = proc.stdout.readline()
        print(output_line.rstrip())

        # if exitted, raise an exception.
        if proc.poll() is not None:
            raise Exception('MongoDB server failed to start up properly.')

        # detect when the server is accepting connections
        if 'waiting for connections' in output_line:
            break

    # Obtain the local port to which Docker mapped Mongo
    cmd = shlex.split('docker ps --filter "name={}" --format '
                      '"{{{{ .Ports }}}}"'.format(_MONGODB_INSTANCE_NAME))
    port_map = subprocess.check_output(cmd, universal_newlines=True).strip()
    port = port_map.split('->', 1)[0].split(':', 1)[1]

    return port


def stop_mongo(let_fail=False):
    """Shutdown the container started by ``start_mongo()``.
    Raise an exception if this operation fails, unless ``let_fail``
    evaluates to True.
    """
    try:
        print('Stopping MongoDB server...')
        cmd = shlex.split('docker rm -vf {}'.format(_MONGODB_INSTANCE_NAME))
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        if not let_fail:
            raise
