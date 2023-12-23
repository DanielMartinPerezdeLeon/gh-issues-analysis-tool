from sqlalchemy import text
import config
from sqlalchemy.engine import create_engine, Engine

engine: Engine


def __init__():
    global engine
    engine = create_engine(config.config_values['database_url'], echo=True)


def set_schema():
    schema_name = f"github_{config.config_values['repository_name']}"

    with engine.connect() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema_name};'))
        result = con.execute(text("SELECT schema_name FROM information_schema.schemata"))


def test():
    config.load_config()
    __init__()
    set_schema()


if __name__ == "__main__":
    test()
