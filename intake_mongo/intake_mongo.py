
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

from intake.source import base
import pymongo


class MongoDBSource(base.DataSource):
    def __init__(self, uri, db, collection, connect_kwargs=None,
                 find_kwargs=None, metadata=None):
        """Load data from MongoDB

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
        super(MongoDBSource, self).__init__(container='python',
                                            metadata=metadata)
        self._uri = uri
        self._db = db
        self._collection = collection
        self._connect_kwargs = connect_kwargs or {}
        self._find_kwargs = find_kwargs or {}
        self.collection = None

    def _get_schema(self):
        if self.collection is None:
            mongo = pymongo.MongoClient(self._uri, **self._connect_kwargs)
            self.collection = mongo[self._db][self._collection]

        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=1,  # consider only one partition
                           extra_metadata={})

    def _get_partition(self, _):
        return list(self.collection.find(**self._find_kwargs))

    def _close(self):
        self.collection = None
