import os
import logging
import pandas as pd
from peewee import fn
from collections import defaultdict
from db_classes import InterfaceDB, Shard1,  Shard2, Shard3


class ImportQueryData(object):
    """ This class handles the data from csv file and its interaction with the database """

    def __init__(self, local_shard_files, postgres_instances, connection_db,
                 cols_list, datetime_column):
        self.local_shard_files = local_shard_files
        # List of strings, which represent the csv file names.
        self.postgres_instances = postgres_instances
        # List of strings, which represent the created database instances.
        self.connection_db = connection_db
        # Dictionary of database credentials.
        self.cols_list = cols_list
        # List of strings, which represent the DataFrame columns to be selected.
        self.datetime_column = datetime_column
        # String, which is the column name to be change as datetime object.
        self.interface_db = InterfaceDB(creds_db=self.connection_db)
        # InterfaceDB class object
        self.dict_total_installs = defaultdict(int)
        # Dictionary where the key is the country ISO code
        # and the value is represented by the counts of paid installs.

    def create_shard_dict(self):
        """
        It creates a dictionary where the key is the name of the shard file
        and the value is a list of database name and created Model of
        the database table.
        """
        self.shard_dict = {self.local_shard_files[idx]:
                         [self.postgres_instances[idx], pwmodel]
                     for idx, pwmodel in enumerate([Shard1, Shard2, Shard3])}

    def df_format_to_db(self):
        """
        It reads the csv file and transform the DataFrame into a list of dictionaries
        to be imported to the database.
        """
        # read the DataFrame form csv file
        df = pd.read_csv(os.path.join(os.getcwd(), self.filename))
        # drop possible duplicates in the data
        df.drop_duplicates(inplace=True)
        # transform column to datetime object
        df[self.datetime_column] = pd.to_datetime(df[self.datetime_column])
        # select only the columns in the cols_list
        df = df[self.cols_list]
        # transform the data into a list of dictionary
        self.list_of_dicts = df.T.to_dict().values()

    def query_installs(self):
        """
        It queries from the table model the number of paid installs
        for each country. The counts will be added to a dictionary attribute,
        where the key is represented by the country and the value is the sum
        of paid installs.
        """
        query_shard = self.interface_db.model.select(self.interface_db.model.country,
                                   fn.SUM(self.interface_db.model.installs).alias('count_installs')) \
            .where(
            (self.interface_db.model.created_at.month == 5) &
            (self.interface_db.model.paid == True)) \
            .group_by(self.interface_db.model.country)
        # it adds the installs counts into the dictionary
        for i in query_shard:
            self.dict_total_installs[i.country] += i.count_installs

    def run(self):
        """
        It iterates through the shard_dict for the different shards.
        For each shard it creates the connection to the database and the shard table
        and it uploads the data into the database table.
        Eventually it queries the number of paid installs for country,
        saved in a dictionary.
        """
        self.create_shard_dict()
        for filename, [name_db, shard] in self.shard_dict.items():
            self.filename = filename
            self.name_db = name_db
            self.shard = shard
            self.df_format_to_db()
            self.interface_db.conn_create_tbl(name_db=self.name_db, model=self.shard)
            self.interface_db.upload_data(data=self.list_of_dicts)
            logging.info('data inserted to database for filename: {}'.format(self.filename))
            self.query_installs()
            self.interface_db.drop_truncate_table()