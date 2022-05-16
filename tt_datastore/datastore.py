from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.buckets import BucketManager
from couchbase.management.queries import QueryIndexManager
from couchbase.management.logic.view_index_logic import (
    DesignDocument,
    DesignDocumentNamespace
)
from couchbase.options import ClusterOptions
import couchbase.exceptions
import datetime
import json

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
            connectionString += "?{0}".format(queryStr)

        authenticator = PasswordAuthenticator(username, password)
        self.cluster = Cluster(connectionString, ClusterOptions(authenticator))
        self.bucket = self.cluster.bucket(bucket)
        self.viewManager = self.bucket.view_indexes()
        self.bucketManager = self.cluster.buckets()
        self.queryManager = self.cluster.query_indexes()
        self.collection = self.cluster.bucket(bucket).default_collection()

    def create(self, key, value, **kwargs):
        result = self.collection.insert(key, value, **kwargs)
        return result.success

    def read(self, key, **kwargs):
        try:
            result = self.collection.get(key, **kwargs)
            return result.value
        except couchbase.exceptions.DocumentNotFoundException:
            return

    def read_with_cas(self, key, **kwargs):
        try:
            result = self.collection.get(key, **kwargs)
            return result.value, result.cas
        except couchbase.exceptions.DocumentNotFoundException:
            return None, None

    def lock(self, key, ttl=15, **kwargs):
        result = self.collection.get_and_lock(
            key, datetime.timedelta(seconds=ttl), **kwargs
        )
        return result.value, result.cas

    def unlock(self, key, cas):
        self.collection.unlock(key, cas)

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
        try:
            result = self.collection.remove(key, **kwargs)
            return result.success
        except couchbase.exceptions.DocumentNotFoundException:
            return False

    def get_multi(self, keys, **kwargs):
        # returns a dictionary that maps keys to Couchbase GetResult
        # objects.
        return self.collection.get_multi(keys, **kwargs)

    def view(self, design, view, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION
        kwargs['namespace'] = namespace

        # couchbase v4 requires that the "key" option be json formatted, which
        # is a breaking change from couchbase v3
        for argname in ["key"]:
            arg = kwargs.get(argname)
            if arg:
                try:
                    json.loads(arg)
                except json.JSONDecodeError:
                    kwargs[argname] = json.dumps(arg)

        # the response from bucket.view_query seems to have had a breaking
        # change from v3 to v4, so instead of returning the response directly
        # we return a backwards-compatible class that should work the same
        # for our purposes.
        class ViewRow(object):
            def __init__(self, value, _id):
                try:
                    self.value = json.loads(value)
                except json.JSONDecodeError:
                    self.value = value
                self.id = _id

        rows = self.bucket.view_query(design, view, **kwargs)
        return [ViewRow(row.value, row.id) for row in rows]

    def design_get(self, name, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION
        dd = self.viewManager.get_design_document(name, namespace, **kwargs)
        return dd.as_dict(namespace)

    def design_create(self, ddoc, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION

        for k, v in ddoc.get('options', {}).items():
            kwargs[k] = v

        ddoc['namespace'] = namespace
        ddoc = DesignDocument.from_json(ddoc)
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
        return self.bucketManager.flush_bucket(self.bucket.name)
