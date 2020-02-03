from necrobot.config import Config


class DBWriter(object):
    def __init__(self, schema_name):
        self.schema_name = schema_name if schema_name is not None else Config.MYSQL_DB_NAME

    def tn(self, table_name):
        return '`{}`.`{}'.format(self.schema_name, table_name)
