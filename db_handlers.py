import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

import config
from sqlalchemy.engine import create_engine, Engine

engine: Engine


def init():
    global engine
    engine = create_engine(config.config_values['database_url'], echo=False)


def set_schema():
    schema = config.config_values['schema_name']

    sql = f'CREATE SCHEMA IF NOT EXISTS "{schema}";'

    try:

        with engine.connect() as con:
            con.execute(text(sql))
            con.commit()

    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemyError creating schema '{schema}': {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred creating schema '{schema}': {e}")
        raise

    else:
        logging.info(f'Schema created or already existed: {schema}')


def create_tables():
    schema = config.config_values['schema_name']

    for table, columns in config.tables.items():

        column_definitions = ", ".join([f'"{column}" {data_type}' for column, data_type in columns.items()])

        sql = f'CREATE TABLE IF NOT EXISTS "{schema}.{table}" ({column_definitions});'

        try:

            with engine.connect() as con:
                con.execute(text(sql))
                con.commit()

        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError creating table '{table}': {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred creating schema '{table}': {e}")
            raise

        else:
            logging.info(f'Table created or already existed: {table}')


def main():
    config.main()
    init()
    set_schema()
    create_tables()


if __name__ == "__main__":
    main()
