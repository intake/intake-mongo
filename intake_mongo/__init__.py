from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='mongo',
                                     version=__version__,
                                     container='python',
                                     partition_access=False)

    def open(self, uri, db, collection, _id=False, connect_kwargs=None,
             find_kwargs=None, metadata=None):
        """
        Create MongoDBSource instance

        Parameters
        ----------
        uri: str
            a valid mongodb uri in the form
            '[mongodb:]//host:port'.
            The URI may include authentication information, see
            http://api.mongodb.com/python/current/examples/authentication.html
        db: str
            The database to access
        collection: str
            The collection in the database that will act as source;
        _id: False or None
            If False, remove default "_id" field from output
        connect_kwargs: dict or None
            Parameters passed to the pymongo ``MongoClient``, see
            http://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
            This may include security information such as passwords and
            certificates
        find_kwargs: dict or None
            Parameters passed to the pymongo ``.find()`` method, see
            http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
            This includes filters, choice of fields, sorting, etc.
        metadata: dict
            The metadata to keep
        """
        from intake_mongo.intake_mongo import MongoDBSource
        return MongoDBSource(uri, db, collection, _id=_id,
                             connect_kwargs=connect_kwargs,
                             find_kwargs=find_kwargs,
                             metadata=metadata or {})
