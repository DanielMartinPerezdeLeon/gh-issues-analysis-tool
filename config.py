import os
import logging

config_values = {}


def load_config():
    global config_values

    logging.info('Getting config values from environment variables...')

    config_values = {
        'database_url': os.getenv('database_url'),
        'repository_name': os.getenv('repository_name'),
        'repository_owner': os.getenv('repository_owner'),
    }

    for key, value in config_values.items():
        if not value:
            logging.error(f'Config value for {key} is null.'
                          f' Please set an environmet variable with its name and a value.')

    urls = {
        'api_repository_url': f"https://api.github.com/repos/"
                              f"{config_values.get('repository_owner')}/{config_values.get('repository_name')}",
    }


def test():
    load_config()


if __name__ == "__main__":
    test()
