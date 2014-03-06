from datastore import *
from couchbase import Couchbase
from couchbase.exception import ServerUnavailableException
from inspect import stack
import unittest

CLIENT_TYPE = "couchbase"
BUCKET = "profiles"

class TestDatastore(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.d = Datastore(CLIENT_TYPE, BUCKET)

    def test_create_read_delete_document(self):
        key = "pikachu"
        value = {"color" : "yellow", "type" : "electric"}
        self.d.create(key, value)
        self.d.delete_key(key)
        self.assertFalse(self.d.read(key))

    def test_get_document(self):
        documentA = {"foo": "foo", "bar": "bar"}
        bucketName = BUCKET_B
        key = stack()[0][3]
        self.cb.add_key(bucketName, key, documentA)
        documentB = self.cb.get_key(bucketName, key)
        self.assertEqual(documentA, documentB)
        self.cb.delete_key(bucketName, key)

    def test_replace_document(self):
        documentA = {"foo": "foo", "bar": "bar"}
        documentB = {"foo": "foo", "bar": "foo"}
        bucketName = BUCKET_B
        key = stack()[0][3]
        self.cb.add_key(bucketName, key, documentA)
        self.cb.replace_key(bucketName, key, documentB)
        documentC = self.cb.get_key(bucketName, key)
        self.assertEqual(documentC, documentB)
        self.cb.delete_key(bucketName, key)

    # def test_create_and_delete_bucket(self):
    #     bucketName = "foobar"
    #     self.assertTrue(self.cb.create_bucket(bucketName))
    #     self.assertTrue(self.cb.delete_bucket(bucketName))
    #     self.assertIsNone(self.cb.get_bucket(bucketName))

    def test_get_keys_from_view(self):
        bucketName = BUCKET_A
        viewName = "_design/accountid/_view/account_by_accountid"

        params = {}
        params.update(dict(limit=1))
        params.update(dict(stale="update_after"))

        self.assertIsNotNone(self.cb.get_view(bucketName, viewName, **params))

    def test_get_design_docs(self):
        bucketName = BUCKET_A
        self.assertIsNotNone(self.cb.get_design_docs(bucketName))

    def test_a_create_counter(self):
        bucketName = BUCKET_A
        key = "test-counter-a"
        self.assertIsNotNone(self.cb.increment_key(bucketName, key)[0])
        self.cb.delete_key(bucketName, key)

    def test_increment_counter(self):
        bucketName = BUCKET_A
        key = "test-counter-b"
        self.assertIsNotNone(self.cb.increment_key(bucketName, key)[0])
        self.assertEqual(self.cb.increment_key(bucketName, key)[0], 2)
        self.assertEqual(self.cb.increment_key(bucketName, key)[0], 3)
        self.cb.delete_key(bucketName, key)

    def test_decrement_counter(self):
        bucketName = BUCKET_A
        key = "my-counter-b"
        self.assertIsNotNone(self.cb.increment_key(bucketName, key)[0])
        self.assertEqual(self.cb.increment_key(bucketName, key)[0], 2)
        self.assertEqual(self.cb.increment_key(bucketName, key)[0], 3)
        self.assertEqual(self.cb.decrement_key(bucketName, key)[0], 2)
        self.cb.delete_key(bucketName, key)


if __name__ == '__main__':
    unittest.main()
