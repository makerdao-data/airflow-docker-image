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
from google.cloud import storage
import os, sys

sys.path.append('/opt/airflow/')
from config import BQ_CREDENTIALS


# make a BigQuery query
def bq_query(query):

    client = bigquery.Client()
    query_job = client.query(query)
    rows = list(query_job.result())

    return rows


# upload local file to a cloud bucket
def upload_file(bucket_name, source_file_name, destination_blob_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print("Uploading file {} to {}.".format(source_file_name, bucket_name))

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded.".format(source_file_name))


# create a BigQuery table from csv file stored in cloud bucket
def load_table(dataset, table, uri, fields):

    client = bigquery.Client()

    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField(field['name'], field['type'], field['mode']) for field in fields
    ]
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 0
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(uri, dataset_ref.records(table), job_config=job_config)

    print("Starting job {} for {}".format(load_job.job_id, table))
    load_job.result()
    destination_table = client.get_table(dataset_ref.records(table))
    print("Loaded {} rows.".format(destination_table.num_rows))


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS
