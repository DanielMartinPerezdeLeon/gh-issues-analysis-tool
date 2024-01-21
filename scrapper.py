import logging
import httpx

import config
import db_handlers


def obtain_response(url: str, page: int = 0) -> str or None:

    headers = {
        "Authorization": config.actual_token  # use the token to have more api access
    }

    if page > 1:
        url = url + f'?page={page}'

    logging.debug(url)

    response = httpx.get(url=url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # If len data > 30 means there are more pages, scrape them too and add their info
        if len(data) >= 30:
            data = data + obtain_response(url=url, page=page+1)

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
    logging.info(f'Processing {table}')

    url = config.api_urls.get(table)

    data = obtain_response(url, page=1)

    if data:

        logging.info(f"Data for {table} obtained")

        try:

            db_handlers.set_data(table, data)

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
