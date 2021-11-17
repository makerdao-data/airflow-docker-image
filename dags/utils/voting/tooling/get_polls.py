import requests
from datetime import datetime
from dags.connectors.gcp import bq_query


def _get_polls_data(max_poll, end_time, base_link='https://governance-portal-v2.now.sh'):
    response = requests.get(base_link + '/api/polling/all-polls')
    if response.status_code != 200:
        # This means something went wrong.
        print('API error {}.'.format(response.status_code))
        titles = None
    else:
        titles = list()
        for poll in response.json():

            if int(poll['pollId']) > max_poll:
                if datetime.strptime(poll['startDate'], '%Y-%m-%dT%H:%M:%S.000Z') <= datetime.strptime(
                    end_time, '%Y-%m-%d %H:%M:%S'
                ):
                    endDate_block = bq_query(
                        f"""
                        select max(number)
                        from `bigquery-public-data.crypto_ethereum.blocks`
                        where timestamp <= '{poll['endDate']}' and date(timestamp) = '{poll['endDate'][:10]}';
                    """
                    )

                    titles.append(
                        [
                            poll['pollId'],
                            poll['title'],
                            poll['blockCreated'],
                            poll['startDate'],
                            endDate_block[0][0],
                            poll['endDate'],
                            str(poll['options']),
                            None,
                        ]
                    )

    return titles
