from airflow.models.baseoperator import BaseOperator
from mongo_plugin.hooks.mongo_hook import MongoHook

class FileSystemToMongoOperator(BaseOperator):
    def __init__(
        self, name: str, 
        mongo_conn_id: str,
        mongo_collection: str,
        mongo_method: str='insert',
        mongo_db: str=None,
        **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.mongo_conn_id = mongo_conn_id
        self.mongo_method = mongo_method
        self.mongo_collection = mongo_collection
        self.mongo_db = mongo_db

        if self.mongo_method not in ('insert', 'replace'):
            raise Exception('Please specify either "insert" or "replace" for Mongo method')
        
    def execute(self, context: Context) -> Any:
        return super().execute(context)