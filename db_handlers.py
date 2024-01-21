import logging
import config
from pymongo import MongoClient

engine: MongoClient


def init():
    global engine
    engine = MongoClient(config.config_values['database_url'])[config.config_values['repository_name']]


# overwrite all data
def set_data(collection: str, data: dict):
    engine[collection].insert_many(data)

    logging.info(f'Data overwritten in {collection}')


def main():
    config.main()
    init()


if __name__ == "__main__":
    main()
