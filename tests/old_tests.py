from src.datastore import *
import unittest
from mock import patch

CLIENT_TYPE = "couchbase"
BUCKET = "profiles"
KEY = "pikachu"
VALUE = {"color" : "yellow", "type" : "electric"}
BAD_CLIENT_TYPE = "chairbase"
BAD_BUCKET = "wrong"
BAD_VALUE = object

class TestDatastore(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.d = Datastore(CLIENT_TYPE, BUCKET)

    def test_connection_error(self):
        with patch('src.datastore.log') as mock_logger:
            self.d = Datastore(CLIENT_TYPE, BAD_BUCKET)
        self.assertTrue(mock_logger.error.called)

    def test_client_type_not_found_error(self):
        self.assertRaises(ClientTypeNotFound, Datastore, (BAD_CLIENT_TYPE, BUCKET))

    def test_create_and_delete_document(self):
        response = self.d.create(KEY, VALUE)
        self.assertTrue(response)
        response = self.d.delete(KEY)
        self.assertTrue(response)

    def test_create_document_error(self):
        with patch('src.datastore.log') as mock_logger:
            self.d.create(KEY, BAD_VALUE)
        self.assertTrue(mock_logger.error.called)

    def test_delete_document_error(self):
        with patch('src.datastore.log') as mock_logger:
            self.d.delete(BAD_VALUE)
        self.assertTrue(mock_logger.error.called)

    def test_read_document(self):
        value = {"color" : "yellow", "type" : "electric"}
        self.d.create(KEY, VALUE)
        response = self.d.read(KEY)
        self.assertEqual(value,response)
        self.assertTrue(response)
        response = self.d.delete(KEY)
        self.assertTrue(response)

    def test_not_found_error(self):
        response = self.d.read(KEY)
        self.assertFalse(response)
        response = self.d.delete(KEY)
        self.assertFalse(response)
        response = self.d.update(KEY, VALUE)
        self.assertFalse(response)

    def test_read_document_error(self):
        with patch('src.datastore.log') as mock_logger:
            self.d.read(BAD_VALUE)
        self.assertTrue(mock_logger.error.called)

    def test_update_document(self):
        initial_value = {"color" : "blue", "type" : "water"}
        updated_value = {"color" : "yellow", "type" : "electric"}
        self.d.create(KEY, initial_value)
        response = self.d.update(KEY, updated_value)
        self.assertTrue(response)
        response = self.d.delete(KEY)
        self.assertTrue(response)

    def test_update_document_error(self):
        self.d.create(KEY, VALUE)
        with patch('src.datastore.log') as mock_logger:
            self.d.update(KEY, BAD_VALUE)
        self.assertTrue(mock_logger.error.called)
        self.d.delete(KEY)

if __name__ == '__main__':
    unittest.main()
