import os
import logging

config_values = {}
api_urls = {}
tables = {}


# Config the logging
def configure_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('httpx').setLevel(logging.ERROR)


# Load all env vars into the script
def load_env():
    global config_values

    configure_logging()
    logging.info('Getting config values from environment variables...')

    # todo borrar esto luego
    os.environ['database_url'] = 'postgresql://postgres:postgres@localhost:5432/local'
    os.environ['repository_name'] = 'helpdesk-validator'
    os.environ['repository_owner'] = 'INSPIRE-MIF'

    config_values = {
        'database_url': os.getenv('database_url'),
        'repository_name': os.getenv('repository_name'),
        'repository_owner': os.getenv('repository_owner'),
        'schema_name': f"github_{os.getenv('repository_name')}"
    }

    for key, value in config_values.items():
        if not value:
            logging.error(f'Config value for {key} is null.'
                          f'Please set an environmet variable with its name and a value.'
                          f'e.g: export repository_name=gh-issues-analysis-tool')
    else:
        logging.info("All config values read succesfully")
        logging.debug(config_values)


def load_urls():
    global api_urls

    base_url = f"https://api.github.com/repos/{config_values.get('repository_owner')}/{config_values.get('repository_name')}"

    api_urls = {

        # 'issues': base_url + "/issues",

        'labels': base_url + "/labels",

        # forks,

        # collaborators,

        # 'tags': base_url + "/tags",

        'stargazers': base_url + "/stargazers",

        'contributors': base_url + '/contributors'
    }


def main():
    load_env()
    load_urls()


if __name__ == "__main__":
    main()
