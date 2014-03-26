import json
import sys
import logging
log = logging.getLogger(__name__)

import couchbase
from couchbase.exceptions import NotFoundError


class ClientBase(object):
    shortcut = None

    def __init__(self, host, port, account, accountPassword, database, databasePassword):
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
    shortcut= "couchbase"

    def __init__(self, host, port, account, accountPassword, database, databasePassword):
        try:
            self.connection = couchbase.Couchbase.connect(host=host, port=port, bucket=database, password=databasePassword)
        except:
            error = sys.exc_info()
            log.error("unexpected error, %s, %s" % (error[0], error[1]))

    def create(self, key, value):
        try:
            self.connection.add(key, value)
            return True
        except:
            error = sys.exc_info()
            log.error("unexpected error, %s, %s" % (error[0], error[1]))
            return False

    def read(self, key):
        try:
            doc = self.connection.get(key)
            return doc.value
        except NotFoundError:
            return False
        except:
            error = sys.exc_info()
            log.error("unexpected error, %s, %s" % (error[0], error[1]))
            return None

    def update(self, key, value):
        try:
            self.connection.get(key)
            self.connection.set(key,value)
            return True
        except NotFoundError:
            return False
        except:
            error = sys.exc_info()
            log.error("unexpected error, %s, %s" % (error[0], error[1]))
            return False

    def delete(self, key):
        try:
            self.connection.delete(key)
            return True
        except NotFoundError:
            return False
        except:
            error = sys.exc_info()
            log.error("unexpected error, %s, %s" % (error[0], error[1]))
            return False

    def view(self, design, view):
        return client.query(design, view)

class Datastore(object):
    client = None

    def __init__(self, clientType, database=None, databasePassword=None, host="localhost", port=8091, account=None, accountPassword=None):
        clientTypes = ClientBase.__subclasses__()
        for ct in clientTypes:
            #two checks: one for clientType as a class (ex: ClientCouchbase), one for clientType as a string ("couchbase")
            if ct == clientType or ct.__name__ == clientType or ct.shortcut == clientType:
                self.client = ct(host=host, port=port, account=account, accountPassword=accountPassword, database=database, databasePassword=databasePassword)

        if self.client == None:
            raise ClientTypeNotFound(clientType)

    def create(self, key, value):
        return self.client.create(key, value)

    def read(self, key):
        return self.client.read(key)

    def update(self, key, value):
        return self.client.update(key, value)

    def delete(self, key):
        return self.client.delete(key)

    def view(self, design, view):
        self.client.query(design, view)


class ClientTypeNotFound(Exception):
    def __init__(self, clientType):
        clientsArray = ClientBase.__subclasses__()
        clientsString = ""
        for c in clientsArray:
            clientsString += "\n%s" % (str(c.__name__))
        
        self.message = "\nCould not find the client type: %s. \
                       \nIf you need it, here is a list of the available client types: %s\n" \
                        % (clientType, clientsString)
    def __str__(self):
        return self.message

           
