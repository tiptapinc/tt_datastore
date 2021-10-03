from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
from couchbase.management.buckets import BucketManager
from couchbase.management.queries import QueryIndexManager
from couchbase.management.views import DesignDocument, DesignDocumentNamespace
import couchbase.exceptions
import datetime

import logging
log = logging.getLogger(__name__)

CONNECTION_KWARGS = [
    # this list is incomplete
    "operation_timeout",
    "config_total_timeout",
    "config_node_timeout"
]


class Datastore(object):
    casException = couchbase.exceptions.CASMismatchException
    lockedException = couchbase.exceptions.DocumentLockedException

    def __init__(self, host, username, password, bucket, **kwargs):
        connectionString = "couchbase://{0}".format(host)

        connectArgs = []
        for arg in CONNECTION_KWARGS:
            value = kwargs.pop(arg, None)
            if value:
                connectArgs.append("{0}={1}".format(arg, value))

        queryStr = "&".join(connectArgs)
        if queryStr:
            connectionString += "/?{0}".format(queryStr)

        authenticator = PasswordAuthenticator(username, password)
        self.cluster = Cluster(connectionString, ClusterOptions(authenticator))
        self.bucket = self.cluster.bucket(bucket)
        self.viewManager = self.bucket.view_indexes()
        self.bucketManager = BucketManager(self.bucket._admin)
        self.queryManager = self.cluster.query_indexes()
        self.collection = self.cluster.bucket(bucket).default_collection()

    def create(self, key, value, **kwargs):
        result = self.collection.insert(key, value, **kwargs)
        return result.success

    def read(self, key, **kwargs):
        result = self.collection.get(key, quiet=True, **kwargs)
        return result.content

    def read_with_cas(self, key, **kwargs):
        result = self.collection.get(key, quiet=True, **kwargs)
        return result.content, result.cas

    def lock(self, key, ttl=15, **kwargs):
        result = self.collection.get_and_lock(
            key, datetime.timedelta(seconds=ttl), **kwargs
        )
        return result.content, result.cas

    def unlock(self, key, cas):
        self.collection.unlock(key, cas, quiet=True)

    def update(self, key, value, **kwargs):
        result = self.collection.replace(key, value, **kwargs)
        return result.success

    def update_with_cas(self, key, value, **kwargs):
        result = self.collection.replace(key, value, **kwargs)
        return result.success, result.cas

    def set(self, key, value, **kwargs):
        result = self.collection.upsert(key, value, **kwargs)
        return result.success

    def set_with_cas(self, key, value, **kwargs):
        # starting with Couchbase python SDK v3, upsert no longer fails
        # on incorrect CAS, so we need to create our own upsert using
        # replace
        try:
            result = self.collection.replace(key, value, **kwargs)
        except couchbase.exceptions.DocumentNotFoundException:
            result = self.collection.insert(key, value, **kwargs)
        return result.success, result.cas

    def delete(self, key, **kwargs):
        result = self.collection.remove(key, quiet=True, **kwargs)
        return result.success

    def get_multi(self, keys, **kwargs):
        # returns a dictionary that maps keys to Couchbase GetResult
        # objects.
        return self.collection.get_multi(keys, **kwargs)

    def view(self, design, view, **kwargs):
        return self.bucket.view_query(design, view, **kwargs)

    def design_get(self, name, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION
        dd = self.viewManager.get_design_document(name, namespace, **kwargs)
        return dd.as_dict(namespace)

    def design_create(self, name, ddoc, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION

        ddoc = DesignDocument.from_json(name, **ddoc)
        self.viewManager.upsert_design_document(
            ddoc, namespace, **kwargs
        )

    def n1ql_index_list(self):
        # I can't get the ViewQueryManager.get_all_indexes method to work,
        # so instead we'll use a n1ql query to get the index list.
        #
        # return self.queryManager.get_all_indexes(self.bucket.name)
        query = (
            'SELECT idx.* FROM system:indexes'
            f' AS idx WHERE keyspace_id = "{self.bucket.name}"'
            ' ORDER BY is_primary DESC, name ASC'
        )
        return self.n1ql_query(query)

    def n1ql_index_create(self, ix, fields, **kwargs):
        # I can't get the ViewQueryManager.create_index method to work,
        # so instead we'll use a n1ql query to create the index.
        #
        # return self.queryManager.create_index(
        #     self.bucket.name, ix, fields, **kwargs
        # )
        if ix in [r['name'] for r in self.n1ql_index_list()]:
            return

        query = (
            f'CREATE INDEX {ix}'
            f' ON {self.bucket.name}({", ".join(fields)})'
            ' WITH {"defer_build": false}'
        )
        results = self.n1ql_query(query)
        list(results)

    def n1ql_query(self, query):
        return self.cluster.query(query)

    def n1ql_index_drop(self, ix):
        if ix in [r['name'] for r in self.n1ql_index_list()]:
            query = f'DROP INDEX {self.bucket.name}.{ix}'
            results = self.n1ql_query(query)
            list(results)

    def flush_bucket(self):
        return self.bucket.flush()
