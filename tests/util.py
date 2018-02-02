import subprocess
import shlex


def verify_plugin_interface(plugin):
    """Assert types of plugin attributes."""
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    """Assert presence of datasource attributes"""

    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)

def start_mongo():
    """Bring up a container running mongo.

    Returns the local port as a string.
    """

    print('Starting MongoDB server...')

    cmd = shlex.split('docker run --rm --name intake-mongodb --publish 27017 '
                      'mongo:latest')
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)

    while True:
        output_line = proc.stdout.readline()
        print(output_line.rstring())

        # if exitted, raise an exception.
        if proc.poll() is not None:
            raise Exception('PostgreSQL server failed to start up properly.')

        # detect when the server is accepting connections
        if 'waiting for connections' in output_line:
            break

    # Obtain the local port to which Docker mapped Mongo
    cmd = shlex.split('docker ps --filter "name=intake-mongodb" --format '
                      '"{{ .Ports }}"')
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
        subprocess.check_call('docker rm -vf intake-mongo')
    except subprocess.CalledProcessError:
        if not let_fail:
            raise
