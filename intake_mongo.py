
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

from intake.source import base
from mongoadapter import MongoAdapter
import pandas
__version__ = '0.0.1'


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
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return MongoDBSource(uri=uri,
                             collection=collection,
                             projection=projection,
                             metadata=base_kwargs['metadata'])


class MongoDBSource(base.DataSource):
    def __init__(self, uri, collection, projection, metadata=None):
        """Load data from MongoDB

        Parameters:
            uri: str
                a valid mongodb uri in the form '[mongodb:]//host:port/database'.
            collection: str
                The collection in the database that will act as source;
            projection: tuple/list
                The fields to query.
            metadate: dict
                The metadata to keep
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
            # perform some checking...
            path = urlparse.unquote(split_url.path).split('/')

            if (split_url.scheme != 'mongodb' or
                    split_url.hostname is None or
                    split_url.port is None or
                    len(path) != 2 or path[0] != '' or
                    split_url.query != '' or
                    split_url.fragment != ''):
                raise Exception()
        except Exception as e:
            new_e = Exception('Unsupported URI for a MongoDB source.'
                              ' Use mongodb://host:port/database')
            new_e.original = e
            raise new_e

        self._uri = uri
        self._host = split_url.hostname
        self._port = int(split_url.port)
        self._database = path[1]  # the path portion pointing to the database
        self._collection = collection
        self._projection = projection
        self._dtypes = None
        self._adapter = None

    def _make_adapter(self):
        self._adapter = MongoAdapter(self._host, self._port, self._database,
                                     self._collection)

    def _get_schema(self):
        if self._adapter is None:
            self._make_adapter()

        # flaky: types could mutate depending on contents of other elements.
        # HOWEVER: Reading the whole dataset would be overkill
        if self._dtypes is None:
            self._dtypes = pandas.DataFrame(
                self._adapter[self._projection][0:10]).dtypes

        return base.Schema(datashape=None,
                           dtype=self._dtypes,
                           shape=(None, len(self._dtypes)),
                           npartitions=1,  # consider only one partition
                           extra_metadata={})

    def _get_partition(self, _):
        return pandas.DataFrame(self._adapter[self._projection][:])

    def _close(self):
        self._adapter = None
