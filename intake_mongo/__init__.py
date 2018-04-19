from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='mongo',
                                     version=__version__,
                                     container='dataframe',
                                     partition_access=False)

        # TODO: The following should be part of "super" initialization.
        self.api_version = 1

    def open(self, uri, collection, projection, **kwargs):
        """
        Create MongoDBSource instance

        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            collection : a mongodb valid query
                mongodb query to be executed.
            projection : a mongodb valid projection
                mongodb projection
            kwargs (dict):
                Additional parameters to pass as keyword arguments to
                ``PostgresAdapter`` constructor.
        """
        from intake_mongo.intake_mongo import MongoDBSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return MongoDBSource(uri=uri,
                             collection=collection,
                             projection=projection,
                             metadata=base_kwargs['metadata'])
