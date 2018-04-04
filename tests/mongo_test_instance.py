#!/usr/bin/env python3
"""Utilities to start and stop a docker instance of mongo for testing.

Two use cases:
- Raise and remove a clean instance to run on automatic tests.

- Start/Stop/Check an instance to be used on interactive tests and when
  developing avoiding the heavy create and destroy cycle that slows down development

As an script, you can use this to start/stop/restart or get info for an mongodb
instance for interactive use. Start/restart/info will echo to stdout the mongodb
uri suited for local use.
"""

import subprocess
import shlex
import contextlib

_TESTING_INSTANCE_NAME = 'intake-mongodb-test'
_INTERACTIVE_INSTANCE_NAME = 'intake-mongodb-test-interactive'
_DATABASE_NAME = 'intake-mongo-test-db'
_DOCKER_IMAGE = 'mongo:latest'


def get_instance(name, dbname):
    """gets the uri for a running named instance
    """
    cmd = shlex.split('docker ps --filter "name={}" --format '
                      '"{{{{ .Ports }}}}"'.format(name))

    try:
        port_map = subprocess.check_output(cmd, universal_newlines=True).strip()
        port = port_map.split('->', 1)[0].split(':', 1)[1]

        return 'mongodb://localhost:{}/{}'.format(port, dbname)
    except Exception:
        return None


def start_instance(name, dbname):
    """Start up a local docker instance with mongodb by the name name

    Returns the uri of the mongo instance (using localhost)
    """

    #publish mongo port
    cmd = shlex.split('docker run --rm --name {} --publish 27017 '
                      '{}'.format(name, _DOCKER_IMAGE))

    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            universal_newlines=True)

    while True:
        output_line = proc.stdout.readline()
        # print(output_line.rstrip())

        #if exitted, raise an exception
        if proc.poll() is not None:
            raise Exception("'{}' failed to start.".format(name))

        if 'waiting for connections' in output_line:
            break

    #obtain the local port that maps to mongo
    uri = get_instance(name, dbname)
    if uri is None:
        raise Exception("The docker instance '{}' did not start propery", name)

    #initialize with the dataset data
    import pymongo
    client = pymongo.MongoClient(uri) #note pymongo ignores the dbname in the uri
    
    #hack: this is used stand-alone as well as inside pytest where it is imported as
    #      a submodule... if local import fails, try global import
    try:
        from . import sample_data
    except ImportError:
        import sample_data

    db = client[dbname]
    for dataset in sample_data.list_datasets():
        db[dataset].insert_many(sample_data.get_dataset(dataset).to_dict('records'))

    return uri


def stop_instance(name, let_fail=False):
    """Shut down the named instance name.

    Raise an exception if the operation fails, unless ``let_fail``
    evaluates to True.
    """

    try:
        cmd = shlex.split('docker rm -vf {}'.format(name))
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        if not let_fail:
            raise


@contextlib.contextmanager
def testing_instance(name=None, dbname=None):
    name = name if name is not None else _TESTING_INSTANCE_NAME
    dbname = dbname if dbname is not None else _DATABASE_NAME
    stop_instance(name, let_fail=True)
    uri = start_instance(name, dbname)

    try:
        yield uri
    finally:
        stop_instance(name)


@contextlib.contextmanager
def interactive_instance(name=None, dbname=None, start_if_needed=False):
    name = name if name is not None else _INTERACTIVE_INSTANCE_NAME
    dbname = dbname if dbname is not None else _DATABASE_NAME

    uri = get_instance(name, dbname)

    if uri is None and start_if_needed:
        uri = start_instance(name, dbname)

    yield uri
    # this is a context manager to be interchangeable with the testing_instance
    # context manager. However, cleaning up the running (or started) instance is
    # not wanted.


if __name__ == '__main__':
    import sys

    name = _INTERACTIVE_INSTANCE_NAME
    dbname = _DATABASE_NAME

    if len(sys.argv) == 2:
        if sys.argv[1] == 'start':
            try:
                instance_uri = start_instance(name, dbname)
            except Exception as e:
                print(e.msg)
                exit(0)
            print(instance_uri)
            exit(1)

        elif sys.argv[1] == 'stop':
            try:
                stop_instance(name)
            except Exception:
                print("Failed to stop the instance. Is it running?")
                exit(0)
            print("Stopped.")
            exit(1)
            
        elif sys.argv[1] == 'restart':
            stop_instance(name, let_fail=True)
            try:
                instance_uri = start_instance(name, dbname)
            except Exception as e:
                print(e.msg)
                exit(0)
            print(instance_uri)
            exit(1)

        elif sys.argv[1] == 'info':
            instance_uri = get_instance(name, dbname)
            if instance_uri is None:
                print("Instance not running")
                exit(0)
            print(instance_uri)
            exit(1)

    print(sys.argv)
    print("usage: {} [start|stop|restart|info]".format(sys.argv[0]))
    exit(0)
