Quickstart
==========

``intake-mongo`` provides quick and easy access to tabular data stored in
`MongoDB`_

.. _MongoDB: https://www.mongodb.com/

This plugin reads MongoDB collections without random access: there is only ever
a single partition.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-mongo

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_mongo``
will become available. It can be used to fetch a collection from the MongoDB
server, and download the results as a list of dictionaries.

Three parameters are of interest when defining a data source:

-  uri: a string like ``'mongodb://localhost:27017'`` to reach the server on
-  collection: a string like ``'test_collection'`` identifying a dataset on the server
-  projection: a string, dee the `mongo docs`_

.. _mongo docs: https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/


Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_mongo

and entries must specify ``driver: mongo``.



Using a Catalog
~~~~~~~~~~~~~~~

A full entry might look like::


    sources:
      sample1:
        driver: mongo
        args:
          uri: 'mongodb://localhost:27017'
          collection: test_collection
