#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Data Services ETL connector for Google Cloud Storage and BigQuery services

from google.cloud import bigquery
import os, sys

sys.path.append('/opt/airflow/')
from config import BQ_CREDENTIALS


# make a BigQuery query
def bq_query(query):

    client = bigquery.Client()
    query_job = client.query(query)
    rows = list(query_job.result())

    return rows


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS
