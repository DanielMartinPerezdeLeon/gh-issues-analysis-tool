import logging
import config, db_handlers
import httpx
import pandas as pd

generic_transformations = [
    'labels', 'stargazers', 'contributors'
]


def transform_generic(df):
    return df


def obtain_response(url: str) -> str or None:
    response = httpx.get(url)

    if response.status_code == 200:
        data = response.json()
        logging.debug(data)
        return data
    elif response.status_code == 404 or response.status_code == 304:
        logging.info(f"No data on {url}")
    elif response.status_code == 403:
        #todo handle
        logging.info(f"API request superated, come back later")
    else:
        logging.error(f"Request failed with status code {response.status_code}: {response.text}")
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")


def upload_dataframe(url: str) -> str or None:
    response = httpx.get(url)

    if response.status_code == 200:
        data = response.json()
        logging.debug(data)
        return data
    elif response.status_code == 404:
        return None
    else:
        logging.error(f"Request failed with status code {response.status_code}: {response.text}")
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")


def process_table(table: str):
    url = config.api_urls.get(table)
    data = obtain_response(url)

    if data:

        try:
            df = pd.DataFrame(data)

            df = df if table in generic_transformations else globals().get(f'transform_{table}', None)

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
        logging.error(f'No data found for {table} (404)')


def test():
    config.main()
    db_handlers.init()
    for table in config.api_urls:
        process_table(table)


if __name__ == "__main__":
    test()
