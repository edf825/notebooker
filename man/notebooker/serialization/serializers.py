from pymongo import MongoClient
from man.notebooker.serialization.mongo import NotebookResultSerializer


class PyMongoNotebookResultSerializer(NotebookResultSerializer):
    def __init__(self,
                 user,
                 password,
                 database_name,
                 mongo_host,
                 result_collection_name='NOTEBOOK_OUTPUT',
                 **kwargs):
        self.user = user
        self.password = password
        super(PyMongoNotebookResultSerializer, self).__init__(database_name, mongo_host, result_collection_name)

    def setup_mongo_connection(self):
        return MongoClient(self.mongo_host, username=self.user, password=self.password).get_database(self.database_name)


class MongooseNotebookResultSerializer(NotebookResultSerializer):
    def __init__(self,
                 database_name='mongoose_notebooker',
                 mongo_host='research',
                 result_collection_name='NOTEBOOK_OUTPUT',
                 **kwargs):
        super(MongooseNotebookResultSerializer, self).__init__(database_name, mongo_host, result_collection_name)

    def setup_mongo_connection(self):
        from ahl.mongo.auth import authenticate
        from ahl.mongo import Mongoose
        from mkd.auth.mongo import get_auth
        mongo = Mongoose(self.mongo_host)._conn[self.database_name]
        user_creds = get_auth(self.mongo_host, 'mongoose', self.database_name)
        authenticate(mongo, user_creds.user, user_creds.password)
        return mongo
