import os

from flask import g

from man.notebooker.serialization.mongoose import NotebookResultSerializer


def get_fresh_serializer():
    # type: () -> NotebookResultSerializer
    return NotebookResultSerializer(
        mongo_host=os.environ['MONGO_HOST'],
        database_name=os.environ['DATABASE_NAME'],
        result_collection_name=os.environ['RESULT_COLLECTION_NAME'])


def get_serializer():
    # type: () -> NotebookResultSerializer
    if not hasattr(g, 'notebook_serializer'):
        g.notebook_serializer = get_fresh_serializer()
    return g.notebook_serializer
