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


    def open(self, uri, collection, query=None, projection=None, **kwargs):
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
                             query=query,
                             projection=projection,
                             metadata=base_kwargs['metadata'])


class MongoDBSource(base.DataSource):
    def __init__(self, uri, collection, query=None, projection=None, metadata=None):
        super(MongoDBSource, self).__init__(container='dataframe',
                                            metadata=metadata)

        self._init_args = {
            'uri': uri,
            'collection': collection,
            'query': query,
            'projection': projection,
        }

        self._uri = uri
        self._collection = collection
        self._query = query
        self._projection = projection
        self._dataframe = None

        # We probably want name and description set via an argument.
        # container on the other hand should be descriptive.
        self.name = 'unnamed'
        self.description = None
        
    def _get_schema(self):
        pass

    def _get_partition(self, _):
        pass

    def _close(self):
        pass
