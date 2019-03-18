import pytest

from ahl.mongo import Mongoose
from arctic.store.bson_store import BSON_STORE_TYPE

pytest_plugins = ['ahl.testing.pytest.mongo_server']

TEST_DB_NAME = 'mongoose_restest'
TEST_LIB = 'NB_OUTPUT'


@pytest.fixture
def bson_library(mongo_server, mongo_host):
    m = Mongoose(mongo_host)
    m.initialize_library(TEST_LIB, BSON_STORE_TYPE)
    l = m.get_library(TEST_LIB)
    return l
