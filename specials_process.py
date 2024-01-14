import logging
import httpx
import pandas as pd

import config
import db_handlers


def get_issue(issue_number: int):
    headers = {
        "Authorization": config.actual_token  # use the token to have more api access
    }

    url = f"{config.api_urls.get('base')}/issues/{issue_number}"

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
    else:
        logging.error(f"Request failed with status code {response.status_code}: {response.text} for {url}")

    return None


def process_issues(df: pd.DataFrame):
    last_issue_n = int(str(df['url'].values[0]).split('/')[-1])

    logging.info(f'\t {last_issue_n} issues in repository')

    for number in range(last_issue_n):
        data = get_issue(issue_number=number+1)

        # Issue process
        data['number'] = number + 1
        data['user_name'] = data.get('user').get('login')
        data['user_id'] = data.get('user').get('id')
        data['labels_name'] = ', '.join([label.get('name') for label in data.get('labels')])
        data['labels_id'] = ', '.join([str(label.get('id')) for label in data.get('labels')])
        data['assignees_name'] = ', '.join([assignee.get('login') for assignee in data.get('assignees')])
        data['assignees_id'] = ', '.join([str(assignee.get('id')) for assignee in data.get('assignees')])
        data.pop('assignee', None)
        data.pop('assignees', None)
        data.pop('reactions', None)  # what would anybody need this for?
        data.pop('user', None)
        data.pop('labels', None)

        #todo depending on the url this changes (maybe change to mongodb???)

        df_issue = pd.DataFrame(data, index=[data['number']])

        df_issue.to_sql(con=db_handlers.engine, index=False, index_label='number',
                        schema=config.config_values.get('schema_name'), name='issues', if_exists='append')

        logging.info(f'\tissue {number + 1} processed')

        # if not df_issue.empty:
        #     df
