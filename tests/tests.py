import pdb
import pytest
import yaml

from tt_datastore import Datastore

RANDOM_DOCS = [
    {
        "docType": "dt1",
        "phrase": "breaks credits"
    }, {
        "docType": "dt1",
        "phrase": "scores alignment"
    }, {
        "docType": "dt1",
        "phrase": "float peace"
    }, {
        "docType": "dt2",
        "phrase": "supply measures"
    }, {
        "docType": "dt2",
        "phrase": "calibration boresight"
    }, {
        "docType": "dt2",
        "phrase": "weaves delimiters"
    }, {
        "docType": "dt2",
        "phrase": "users mattresses"
    }, {
        "docType": "dt2",
        "phrase": "mule maneuver"
    }, {
        "docType": "dt1",
        "phrase": "saturdays membranes"
    }, {
        "docType": "dt1",
        "phrase": "wreck byte"
    }
]

DOC_KEYS = [f"doc_{i}" for i in range(len(RANDOM_DOCS))]


@pytest.fixture(scope='module')
def datastore():
    return Datastore(
        "couchbase", "Administrator", "password", "test"
    )


@pytest.fixture(autouse=True, scope='module')
def setup_bucket(datastore):
    datastore.flush_bucket()
    for k, v in zip(DOC_KEYS, RANDOM_DOCS):
        datastore.create(k, v)


@pytest.fixture()
def design_doc():
    with open("tests/test_design_doc.yml") as f:
        return yaml.safe_load(f)


def test_init_datastore(datastore):
    assert isinstance(datastore, Datastore)


def test_delete(datastore):
    key = "test_delete"
    value = {"this": "doesn't matter"}

    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value

    success = datastore.delete(key)
    assert success is True

    value = datastore.read(key)
    assert not value


def test_delete_nonexistent(datastore):
    key = "test_delete"
    value = {"this": "still doesn't matter"}

    success = datastore.set(key, value)
    assert success is True
    datastore.delete(key)

    success = datastore.delete(key)
    assert success is False


def test_create_and_read_str(datastore):
    key = "test_string"
    value = "the quick brown fox jumps over the lazy dog"

    datastore.delete(key)  # ensure it's not already there
    success = datastore.create(key, value)

    assert success is True

    readvalue = datastore.read(key)
    assert isinstance(readvalue, str)
    assert readvalue == value


def test_create_and_read_dict(datastore):
    key = "test_dict"
    value = {"yo": "dawg", "fish": "marlin", "repo man": "intense"}

    datastore.delete(key)  # ensure it's not already there
    success = datastore.create(key, value)

    assert success is True

    readvalue = datastore.read(key)
    assert isinstance(readvalue, dict)
    assert readvalue == value


def test_create_preexisting(datastore):
    key = "test_string"
    value = "the quick brown fox has a belly ache"

    with pytest.raises(Exception):
        datastore.create(key, value)


def test_read_nonexistent(datastore):
    key = "test_string"
    datastore.delete(key)  # ensure it's not already there

    success = datastore.read(key)
    assert not success


def test_read_with_cas(datastore):
    key = "test_string"
    value = "the quick brown fox jumps on the lazy dog"

    success = datastore.set(key, value)
    assert success is True

    readvalue, readcas = datastore.read_with_cas(key)
    assert readvalue == value
    assert bool(readcas)    # ensure it's not nothing


def test_lock_and_unlock(datastore):
    key = "lock_key"
    value = "the quick brown fox double backflips over the lazy dog"

    success = datastore.set(key, value)

    readvalue, cas = datastore.lock(key, ttl=30)
    assert readvalue == value
    assert bool(cas)
    newvalue = "the quick brown fox falls asleep"
    with pytest.raises(datastore.lockedException):
        datastore.update(key, newvalue)
    datastore.unlock(key, cas)

    readvalue, cas = datastore.lock(key, ttl=30)
    assert readvalue == value
    assert bool(cas)
    with pytest.raises(datastore.lockedException):
        datastore.set(key, newvalue)
    datastore.unlock(key, cas)

    readvalue, cas = datastore.lock(key, ttl=30)
    assert readvalue == value
    assert bool(cas)
    with pytest.raises(datastore.lockedException):
        datastore.delete(key)
    datastore.unlock(key, cas)

    readvalue, cas = datastore.lock(key, ttl=30)
    assert readvalue == value
    assert bool(cas)
    with pytest.raises(datastore.lockedException):
        datastore.unlock(key, 55)
    datastore.unlock(key, cas)

    readvalue, cas = datastore.lock(key, ttl=30)
    assert readvalue == value
    assert bool(cas)
    datastore.unlock(key, cas)
    success = datastore.update(key, newvalue)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_update(datastore):
    key = "test_string"
    value = "the quick brown fox laughs at the lazy dog"

    success = datastore.set(key, value)
    assert success is True

    newvalue = value.replace("fox", "aardvark")
    success = datastore.update(key, newvalue)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_update_with_cas(datastore):
    key = "test_string"
    value = "the quick brown fox wiggles under the lazy dog"

    datastore.delete(key)  # ensure it's not already there
    success, cas = datastore.set_with_cas(key, value)
    assert success is True

    newvalue = "the slow red fox jumps on top of the crazy dog"
    with pytest.raises(datastore.casException):
        datastore.update_with_cas(key, newvalue, cas=55)

    readvalue = datastore.read(key)
    assert readvalue == value

    success, newcas = datastore.update_with_cas(key, newvalue, cas=cas)
    assert success is True
    assert newcas != cas

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_set(datastore):
    key = "test_string"
    value = "the quirky brown fox pole vaults the Lays-eating dog"

    datastore.delete(key)
    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value


def test_set_preexisting(datastore):
    key = "test_string"
    value = "the spritely fox bounds over the somnolent dog"

    datastore.delete(key)
    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value

    newvalue = "the alaskan fox shakes hands with the hawaiian dog"
    success = datastore.set(key, newvalue)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_set_with_cas(datastore):
    key = "test_string"
    value = "the floundering fox jumps over the two-dimensional dog"

    datastore.delete(key)  # ensure it's not already there
    success, cas = datastore.set_with_cas(key, value)
    assert success is True

    newvalue = "the floundering fox jumps over the flounder"
    with pytest.raises(datastore.casException):
        datastore.set_with_cas(key, newvalue, cas=55)

    readvalue = datastore.read(key)
    assert readvalue == value

    success, newcas = datastore.set_with_cas(key, newvalue, cas=cas)
    assert success is True
    assert newcas != cas

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_get_multi(datastore):
    phrases = [doc['phrase'] for doc in RANDOM_DOCS]
    keys = [str(i) for i in range(len(phrases))]
    for k, v in zip(keys, phrases):
        datastore.set(k, v)

    multiResult = datastore.get_multi(keys)

    resultsByKey = {k: v for k, v in multiResult.items()}
    values = [r.content for r in resultsByKey.values()]
    assert len(values) == len(phrases)
    assert set(values) == set(values)

    casses = [r.cas for r in resultsByKey.values()]
    assert len(casses) == len(keys)

    assert set(resultsByKey.keys()) == set(keys)


def test_design_create_and_get(datastore, design_doc):
    for docName, doc in design_doc.items():
        datastore.design_create(docName, doc, use_devmode=False)
        result = datastore.design_get(docName, use_devmode=False)
        assert result['views'] == doc['views']


def test_view(datastore, design_doc):
    doc = design_doc["test"]
    datastore.design_create("test", doc, use_devmode=False)

    rows = datastore.view("test", "test_1", key="dt1", stale=False)
    phrases = [row.value for row in rows]
    expectedPhrases = [
        doc["phrase"]
        for doc in RANDOM_DOCS
        if doc["docType"] == "dt1"
    ]
    assert set(phrases) == set(expectedPhrases)


def test_n1ql_index_create_and_list(datastore):
    datastore.n1ql_index_drop("test")
    datastore.n1ql_index_create(
        "test",
        ["docType", "phrase"],
        ignore_if_exists=True
    )
    idxes = datastore.n1ql_index_list()
    idxnames = [idx['name'] for idx in idxes]
    assert "test" in idxnames


def test_n1ql_query(datastore):
    datastore.n1ql_index_create(
        "test",
        ["docType", "phrase"],
        ignore_if_exists=True
    )
    query = 'SELECT phrase FROM test WHERE docType = "dt2"'
    results = datastore.n1ql_query(query)
    phrases = [r["phrase"] for r in results]
    expectedPhrases = [
        doc["phrase"]
        for doc in RANDOM_DOCS
        if doc["docType"] == "dt2"
    ]
    assert set(phrases) == set(expectedPhrases)
