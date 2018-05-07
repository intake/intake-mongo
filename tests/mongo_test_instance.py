import random
import pymongo
import subprocess
import shlex
import time

DB = 'intake-mongo-test-db'
COLL = 'test-collection'
URI = 'mongodb://localhost:27017'
data = [{'field1': 1, "name": "aname", 'value': random.random()}
        for _ in range(100)]


def start_mongo():
    """Bring up a container running Mongo.

    Waits until REST API is live and responsive.
    """
    stop_docker(let_fail=True)
    print('Starting MongoDB server...')

    cmd = 'docker run -d --name intake-mongo -p 27017:27017 mongo'
    print(cmd)
    # the return value is actually the container ID
    cid = subprocess.check_output(shlex.split(cmd)).strip().decode()

    timeout = 15
    while True:
        try:
            c = pymongo.MongoClient(URI)
            c.list_database_names()
            break
        except:
            time.sleep(0.2)
            timeout -= 0.2
            if timeout < 0:
                raise RuntimeError('timeout waiting for Mongo')
    c[DB][COLL].insert_many([d.copy() for d in data])
    return cid


def stop_docker(name='intake-mongo', cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -a -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
