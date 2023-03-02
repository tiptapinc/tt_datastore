from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.management.queries import QueryIndexManager
from couchbase.management.views import DesignDocument, DesignDocumentNamespace
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
        self.queryManager = self.cluster.query_indexes()
        self.collection = self.cluster.bucket(bucket).default_collection()

    def create(self, key, value, **kwargs):
        result = self.collection.insert(key, value, **kwargs)
        return result.success

    def read(self, key, **kwargs):
        # catch exceptions to emulate the old "quiet=True" behavior
        try:
            result = self.collection.get(key, **kwargs)
        except couchbase.exceptions.DocumentNotFoundException:
            return {}

        return result.value

    def read_with_cas(self, key, **kwargs):
        # catch exceptions to emulate the old "quiet=True" behavior
        try:
            result = self.collection.get(key, **kwargs)
        except couchbase.exceptions.DocumentNotFoundException:
            return {}, None

        return result.value, result.cas

    def lock(self, key, ttl=15, **kwargs):
        result = self.collection.get_and_lock(
            key, datetime.timedelta(seconds=ttl), **kwargs
        )
        return result.value, result.cas

    def unlock(self, key, cas):
        # catch exceptions to emulate the old "quiet=True" behavior
        try:
            self.collection.unlock(key, cas)
        except couchbase.exceptions.DocumentNotFoundException:
            pass

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
        # catch exceptions to emulate the old "quiet=True" behavior
        try:
            result = self.collection.remove(key, **kwargs)
        except couchbase.exceptions.DocumentNotFoundException:
            return False

        return result.success

    def get_multi(self, keys, **kwargs):
        # returns a dictionary that maps keys to Couchbase GetResult
        # objects.
        return self.collection.get_multi(keys, **kwargs)

    def view(self, design, view, **kwargs):
        # "stale" keyword argument changed to "scan_consistency" for couchbase
        # SDK 4.xx
        if 'stale' in kwargs:
            stale = kwargs['stale']
            if isinstance(stale, bool) and stale is False:
                kwargs['scan_consistency'] = "false"
            elif isinstance(stale, str) and stale.lower() in ["ok", "update_after", "false"]:
                kwargs['scan_consistency'] = stale.lower()
            del kwargs['stale']

        # couchbase SDK 4.xx doesn't deserialize row.value and row.id, whereas
        # older couchbase SDKs did. For compatibility purposes return something
        # that looks like the old SDK's result
        class OldStyleRow(object):
            def __init__(self, raw_row):
                try:
                    self.key = json.loads(raw_row.key)
                except json.decoder.JSONDecodeError:
                    self.key = raw_row.key

                self.id = raw_row.id

                try:
                    self.value = json.loads(raw_row.value)
                except json.decoder.JSONDecodeError:
                    self.value = raw_row.value

                self.document = raw_row.document

        raw_rows = self.bucket.view_query(design, view, **kwargs).rows()
        rows = [OldStyleRow(raw_row) for raw_row in raw_rows]
        return rows

    def design_get(self, name, **kwargs):
        if kwargs.pop('use_devmode', False):
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            namespace = DesignDocumentNamespace.PRODUCTION
        dd = self.viewManager.get_design_document(name, namespace, **kwargs)
        return dd.as_dict(namespace)

    def design_create(self, name, ddoc, **kwargs):
        if kwargs.pop('use_devmode', False):
            ddoc['namespace'] = "development"
            namespace = DesignDocumentNamespace.DEVELOPMENT
        else:
            ddoc['namespace'] = "production"
            namespace = DesignDocumentNamespace.PRODUCTION

        ddoc['name'] = name
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
        return self.cluster.buckets().flush_bucket(self.bucket.name)
