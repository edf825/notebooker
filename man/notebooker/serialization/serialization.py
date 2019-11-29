from enum import Enum
import os

from flask import g

from man.notebooker.serialization.serializers import PyMongoNotebookResultSerializer
from man.notebooker.serialization.mongo import NotebookResultSerializer


class Serializer(Enum):
    MONGOOSE = 'MongooseNotebookResultSerializer'
    PYMONGO = 'PyMongoNotebookResultSerializer'


def serializer_kwargs_from_os_envs():
    return {
        'user': os.environ.get('MONGO_USER'),
        'password': os.environ.get('MONGO_PASSWORD'),
        'mongo_host': os.environ.get('MONGO_HOST'),
        'database_name': os.environ.get('DATABASE_NAME'),
        'result_collection_name': os.environ.get('RESULT_COLLECTION_NAME')
    }


def get_serializer_from_cls(serializer_cls, **kwargs):
    # type: (str, dict) -> NotebookResultSerializer

    if serializer_cls == Serializer.MONGOOSE.value:
        from man.notebooker.serialization.serializers import MongooseNotebookResultSerializer
        return MongooseNotebookResultSerializer(**kwargs)
    elif serializer_cls == Serializer.PYMONGO.value:
        return PyMongoNotebookResultSerializer(**kwargs)
    else:
        raise ValueError("Unspported serializer {}".format(serializer_cls))


def get_fresh_serializer():
    # type: () -> NotebookResultSerializer
    serializer_cls = os.environ.get('NOTEBOOK_SERIALIZER', Serializer.MONGOOSE.value)
    serializer_kwargs = serializer_kwargs_from_os_envs()
    return get_serializer_from_cls(serializer_cls, **serializer_kwargs)


def get_serializer():
    # type: () -> NotebookResultSerializer
    if not hasattr(g, 'notebook_serializer'):
        g.notebook_serializer = get_fresh_serializer()
    return g.notebook_serializer
