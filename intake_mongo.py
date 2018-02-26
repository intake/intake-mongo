
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

from intake.source import base
from mongoadapter import MongoAdapter


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='mongo',
                                     version='0.1',
                                     container='dataframe',
                                     partition_access=False)

        # TODO: The following should be part of "super" initialization.
        self.api_version = 1


    def open(self, uri, collection, projection, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            query : a mongodb valid query
                mongodb query to be executed.
            projection : a mongodb valid projection
                mongodb projection
            kwargs (dict):
                Additional parameters to pass as keyword arguments to
                ``PostgresAdapter`` constructor.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return MongoDBSource(uri=uri,
                             collection=collection,
                             projection=projection,
                             metadata=base_kwargs['metadata'])

class MongoDBSource(base.DataSource):
    def __init__(self, uri, collection, projection, metadata=None):
        """arguments are:
        uri: a valid mongodb uri in the form '[mongodb:]//host:port/database'.
        collection: The collection in the database that will act as source.
        projection: A tuple-like with the fields to query.
        metadate: The metadata to keep
        """
        super(MongoDBSource, self).__init__(container='dataframe',
                                            metadata=metadata)

        self._init_args = {
            'uri': uri,
            'collection': collection,
            'projection': projection,
        }


        try:
            split_url = urlparse.urlsplit(uri, scheme='mongodb')
            #perform some checking...
            path = urlparse.unquote(split_url.path).split('/')

            if (split_url.scheme != 'mongodb' or
                split_url.hostname is None or
                split_url.port is None or
                len(path) != 2 or path[0] != '' or
                split_url.query != '' or
                split_url.fragment != ''):
                raise Exception()
        except Exception as e:
            new_e = Exception('Unsupported URI for a MongoDB source. Use mongodb://host:port/database')
            new_e.original = e
            raise new_e

        self._uri = uri
        self._host = split_url.hostname
        self._port = int(split_url.port)
        self._database = path[1] # the path portion pointing to the database
        self._collection = collection
        self._projection = projection
        self._closed = None # strictly it hasn't been closed, but it is not 'open' either.

        # We probably want name and description set via an argument.
        # container on the other hand should be descriptive.
        self.name = 'unnamed'
        self.description = None
    

    def _init_mongo_adapter(self):
        #initialize mongo adapter using the information provider
        print(self._host, self._port, self._database, self._collection)
        return MongoAdapter(self._host, self._port, self._database, self._collection)


    def _get_schema(self):
        print(self._adapter, self._projection)
        print(self._adapter[self._projection][0])
        dtype = self._adapter[self._projection][0].dtype

        # workaround to partition_map not added in base...
        self.partition_map = None

        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=(None,), # length is unknown till read
                           npartitions=1, # consider only one partition
                           extra_metadata={})


    def _get_partition(self, _):
        return self._adapter[self._projection][:]


    def __getattr__(self, name):
        if name == '_adapter':
            #initialize the adapter
            self._closed  = False
            self._adapter = self._init_mongo_adapter()
            return self._adapter
        else:
            raise AttributeError(name)


    def _close(self):
        if self._closed == False:
            del self.partition_map
            del self._adapter

        self._closed = True


