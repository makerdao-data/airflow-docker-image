import sys
sys.path.append('/opt/airflow/')
from dags.connectors.gcp import bq_query


def _get_max_block(date):

    q = f"""
        SELECT MAX(number)
        FROM `bigquery-public-data.crypto_ethereum.blocks`
        WHERE DATE(timestamp) = "{date}";
    """

    max_block = bq_query(q)[0][0]

    return max_block