import os
import logging

config_values = {}
api_urls = {}
actual_token = str


# Config the logging
def configure_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('httpx').setLevel(logging.ERROR)


# Load all env vars into the script
def load_env():
    global config_values
    global actual_token

    configure_logging()
    logging.info('Getting config values from environment variables...')

    # todo borrar esto luego
    os.environ['database_url'] = 'postgresql://postgres:postgres@localhost:5432/local'
    os.environ['repository_name'] = 'helpdesk-validator'
    os.environ['repository_owner'] = 'INSPIRE-MIF'
    os.environ['tokens_list'] = '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'

    config_values = {
        'database_url': os.getenv('database_url'),
        'repository_name': os.getenv('repository_name'),
        'repository_owner': os.getenv('repository_owner'),
        'schema_name': f"github_{os.getenv('repository_name')}",
        'tokens_list': os.getenv('tokens_list').split(',')
    }

    actual_token = config_values.get('tokens_list')[0]

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

        'base': base_url,

        'generics': {
            'stargazers': base_url + "/stargazers",
            'contributors': base_url + '/contributors',
            'labels': base_url + "/labels",
            'collaborators': base_url + '/collaborators',
            'subscribers': base_url + '/subscribers',
            'subscription': base_url + '/subscription',
            'watchers': base_url + '/watchers'
        },

        'specials': {
            'issues': base_url + "/issues?per_page=1",
            # forks,
            # 'tags': base_url + "/tags",
        }

    }


# Change the token used for the next one to not get limited by the api
def change_actual_token():
    global actual_token

    tokens_list = config_values.get('tokens_list')

    next_index = tokens_list.index(actual_token) + 1

    if next_index >= len(tokens_list):
        logging.info('Out of tokens, returning to the first one')
        actual_token = tokens_list[0]
    else:
        actual_token = tokens_list[next_index]


def main():
    load_env()
    load_urls()


if __name__ == "__main__":
    main()
