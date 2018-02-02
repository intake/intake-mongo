from intake.source import base

from mongoadapter import MongoAdapter


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='mongo',
                                     version='0.1',
                                     container='dataframe',
                                     partition_access=False)

    def open(self, uri, query, projection, **kwargs):
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
                             query=query,
                             projection=projection,
                             metadata=base_kwargs['metadata'])


class MongoDBSource(base.DataSource):
    def __init__(self, uri, query, projection, metadata):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'pg_kwargs': pg_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._query = query
        self._projection = projection
        self._dataframe = None

        super(PostgresSource, self).__init__(container='dataframe',
                                             metadata=metadata)

    def _get_schema(self):
        pass

    def _get_partition(self, _):
        pass

    def _close(self):
        pass
