import json
import traceback

import logging
log = logging.getLogger(__name__)

import couchbase
from couchbase.exceptions import NotFoundError


class ClientBase(object):
    shortcut = None

    def __init__(self, host, port, database, databasePassword):
        pass

    def create(self):
        pass

    def read(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class ClientCouchbase(ClientBase):
    shortcut = "couchbase"

    def __init__(self, host, port, database, databasePassword,
                 jsonEncoder=None):

        if jsonEncoder:
            couchbase.set_json_converters(jsonEncoder, json.loads)

        self.connection = couchbase.Couchbase.connect(
            host=host,
            port=port,
            bucket=database,
            password=databasePassword
        )

    def create(self, key, value):
        try:
            self.connection.add(key, value)
            return True
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def read(self, key):
        try:
            doc = self.connection.get(key)
            return doc.value
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return None

    def update(self, key, value):
        try:
            self.connection.replace(key, value)
            return True
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def set(self, key, value):
        try:
            self.connection.set(key, value)
            return True
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def delete(self, key):
        try:
            self.connection.delete(key)
            return True
        except NotFoundError:
            return False
        except:
            log.error("unexpected error, %s" % traceback.format_exc())
            return False

    def view(self, design, view, **kwargs):
        return self.connection.query(design, view, **kwargs)


class Datastore(object):
    client = None

    def __init__(self, clientType, database=None, databasePassword=None,
                 host="localhost", port=8091, **kwargs):

        clientTypes = ClientBase.__subclasses__()
        for ct in clientTypes:
            # two checks: one for clientType as a class (ex: ClientCouchbase),
            # one for clientType as a string ("couchbase")
            if (
                ct == clientType or
                ct.__name__ == clientType or
                ct.shortcut == clientType
            ):
                self.client = ct(
                    host=host,
                    port=port,
                    database=database,
                    databasePassword=databasePassword,
                    **kwargs
                )

        if self.client is None:
            raise ClientTypeNotFound(clientType)

    def create(self, key, value):
        return self.client.create(key, value)

    def read(self, key):
        return self.client.read(key)

    def update(self, key, value):
        return self.client.update(key, value)

    def set(self, key, value):
        return self.client.set(key, value)

    def delete(self, key):
        return self.client.delete(key)

    def view(self, design, view, **kwargs):
        return self.client.view(design, view, **kwargs)


class ClientTypeNotFound(Exception):
    def __init__(self, clientType):
        clientsArray = ClientBase.__subclasses__()
        clientsString = ""
        for c in clientsArray:
            clientsString += "\n%s" % (str(c.__name__))

        self.message = "\nCould not find the client type: %s. " \
                       "\nIf you need it, here is a list of the " \
                       "available client types: %s\n" % \
                       (clientType, clientsString)

    def __str__(self):
        return self.message
