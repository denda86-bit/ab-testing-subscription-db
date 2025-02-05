import logging
from peewee import IntegerField, Model, CharField, \
    DateTimeField, BooleanField, DatabaseProxy, PostgresqlDatabase, OperationalError
PROXY = DatabaseProxy()


class BasicModel(Model):
    """A base model of PostgresSQL database"""
    country = CharField(db_column='country')
    created_at = DateTimeField(db_column='created_at')
    paid = BooleanField(db_column='paid')
    installs = IntegerField(db_column='installs')


class Shard1(BasicModel):
    class Meta:
        database = PROXY
        db_table = 'SHARD1'


class Shard2(BasicModel):
    class Meta:
        database = PROXY
        db_table = 'SHARD2'


class Shard3(BasicModel):
    class Meta:
        database = PROXY
        db_table = 'SHARD3'


class InterfaceDB:
    """ This class interact with the PostgresSQL database. """
    def __init__(self, creds_db):
        """
        Initialization of class attributes.
        :param creds_db: dict
        Dictionary with credentials for database connection.
        """
        self.host = creds_db['host']
        # String, host database name.
        self.port = creds_db['port']
        # String, port database name.
        self.user = creds_db['user']
        # String, user database name.
        self.password = creds_db['password']
        # String, password database name.
        self.proxy = PROXY
        # placeholder object DatabaseProxy

    def connect_db(self):
        """
        Connection to the PostgresSQL database.
        """
        try:
            self.db = PostgresqlDatabase(self.name_db, user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port)
            self.db.connect()
        except OperationalError as e:
            raise Exception('Unable to connect to database {name_db}!'
                            .format(name_db=self.name_db))
        else:
            logging.info('Connection Successful to database {name_db}.'
                  .format(name_db=self.name_db))

    def initialize_proxy(self):
        """
        It swaps out the placeholder object DatabaseProxy
        at run-time for a different object.
        """
        try:
            PROXY.initialize(self.db)
        except Exception:
            raise Exception('It cannot initialize the proxy as another object.')

    def upload_data(self, data):
        """
        It uploads the data to the database table.
        :param data: list of dicts
        The key of each dictionary is columns name which the dictionary value
        will be added to.
        """
        with self.db.atomic():
            query = self.model.insert_many(data)
            query.execute()

    def drop_truncate_table(self, drop_table=False):
        """
        If drop_table is True it drops the table; otherwise it keeps the table but
        truncate it.
        :param drop_table: bool
        Default is False.
        """
        if drop_table:
            self.model.drop_table()
        else:
            self.model.truncate_table()

    def conn_create_tbl(self, name_db, model):
        """
        It connects to the database, initialize the proxy as table model object
        and create the table in the database.
        """
        self.name_db = name_db
        self.model = model
        self.connect_db()
        self.initialize_proxy()
        self.model.create_table()