import logging
import httpx
import pandas as pd
import importlib


import config
import db_handlers
import specials_process


def transform_generic(df):
    return df


def obtain_response(url: str) -> str or None:
    headers = {
        "Authorization": config.actual_token  # use the token to have more api access
    }

    response = httpx.get(url=url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        logging.debug(data)
        return data
    elif response.status_code == 404 or response.status_code == 304:
        logging.info(f"No data on {url}")
    elif response.status_code == 403:
        # todo handle
        logging.info(f"API request superated, come back later")
    # elif response.status_code == 401:
        # todo handle logins
    else:
        logging.error(f"Request failed with status code {response.status_code}: {response.text} for {url}")

    return None


def process_table(table: str):
    if table in config.api_urls.get('generics'):
        url = config.api_urls.get('generics').get(table)
        generic = True
    else:
        url = config.api_urls.get('specials').get(table)
        generic = False

    data = obtain_response(url)

    if data:

        try:
            df = pd.DataFrame(data)

            # a bit complicated: if the table is in generic it doesn't do anything,
            # BUT if the table is in specials it dinamically call the function process_(table_name)
            # from specials_process
            # crazy stuff
            # if it is not generic nor exists that special process it tells
            df = df if generic else getattr(specials_process, f'process_{table}', None)(df)

            if df is None:
                raise Exception(f"Transformation function for table '{table}' not found."
                                f" Has this url process been created or added to generics?")

            # Upload the transformed DataFrame to the database
            df.to_sql(con=db_handlers.engine, schema=config.config_values.get('schema_name'), name=table,
                      if_exists='replace', index=False, index_label='id')

        except Exception as e:
            logging.error(f'Error processing or uploading {table}')
            logging.error(e)
            raise e

        else:
            logging.info(f'Table {table} processed and uploaded successfully')

    else:
        logging.error(f'{table} NOT uploaded')


def test():
    config.main()
    db_handlers.init()
    # for table in config.api_urls.get('generics'):
    #     process_table(table)
    process_table('issues')


if __name__ == "__main__":
    test()
