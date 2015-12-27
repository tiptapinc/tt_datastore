import traceback

import logging
log = logging.getLogger(__name__)

from couchbase.bucket import Bucket
from couchbase.exceptions import NotFoundError, KeyExistsError


class Datastore(object):

    def __init__(self, host, port, bucket, **kwargs):
        connectionString = "http://%s:%s/%s" % (host, port, bucket)
        self.bucket = Bucket(connectionString, **kwargs)

    def create(self, key, value):
        try:
            self.bucket.insert(key, value)
            return True
        except KeyExistsError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def read(self, key):
        try:
            doc = self.bucket.get(key)
            return doc.value
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return None

    def update(self, key, value):
        try:
            self.bucket.replace(key, value)
            return True
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def set(self, key, value):
        try:
            self.bucket.upsert(key, value)
            return True
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def delete(self, key):
        try:
            self.bucket.remove(key)
            return True
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def view(self, design, view, **kwargs):
        return self.bucket.query(design, view, **kwargs)

    def get_multi(self, keys):
        return self.bucket.get_multi(keys)

    def design_get(self, name, **kwargs):
        return self.bucket.bucket_manager().design_get(name, **kwargs)

    def design_create(self, name, ddoc, **kwargs):
        return self.bucket.bucket_manager().design_create(name, ddoc, **kwargs)
