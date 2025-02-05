import os
import yaml
import logging
from handling_data import ImportQueryData
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


if __name__ == "__main__":
    # upload of yaml dictionaries
    conf_filename = open(os.path.join(os.getcwd(), 'config.yaml'))
    conf_dict = yaml.load(conf_filename)
    # Initialization of class ImportQueryData
    import_query_obj = ImportQueryData(local_shard_files=conf_dict['local_shard_files'],
                                       postgres_instances=conf_dict['postgres_instances'],
                                       connection_db=conf_dict['connection_db'],
                                       cols_list=conf_dict['selected_columns'],
                                       datetime_column=conf_dict['datetime_column'])
    # class method where the connection to the Postgres instance is created
    # for the different shards, the database table is created, the data are inserted
    # and eventually the paid installs are queried for each country.
    import_query_obj.run()
    # Logging of the total sum of paid installs for each country for all the shards.
    logging.info(''.join('\n{cnt_installs} total installs for ISO country code: {country}'
                 .format(cnt_installs=v, country=k)
                 for k, v in import_query_obj.dict_total_installs.items()))