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

import os

# Google BigQuery connection
BQ_PROJECT = 'mcd-265409'
BQ_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# Google Sheets/Drive Service Account
SERV_ACCOUNT = os.environ.get('SERV_ACCOUNT')

# Snowflake connection
SNOWFLAKE_CONNECTION = dict(
    account=os.environ.get('SNOWFLAKE_ACCOUNT'),
    user=os.environ.get('SNOWFLAKE_USER'),
    password=os.environ.get('SNOWFLAKE_PASS'),
    warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
    role=os.environ.get('SNOWFLAKE_ROLE'))

ETHERSCAN_API_KEY = os.environ.get('ETHERSCAN_API_KEY')

# Blockchain node connection
MAINNET_NODE = os.environ.get('MAINNET_NODE')

STAGING = "EXTRACTS"

# GENERAL
dataset = "bigquery-public-data.crypto_ethereum"
etherscan_api_key = os.environ.get('ETHERSCAN_API_KEY')
absolute_import_path = os.environ.get('SYS_PATH')

# VAULTS
vaults_fallback_block = 8928151
vaults_fallback_time = '2019-11-13 00:00:00'
vat_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
manager_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'

vaults_db = os.environ.get('VAULTS_DB')
vaults_scheduler = os.environ.get('VAULTS_SCHEDULER')

staging_vaults_db = os.environ.get('STAGING_VAULTS_DB')
staging_vaults_scheduler = os.environ.get('STAGING_VAULTS_SCHEDULER')

# VOTES
votes_fallback_block = 4749331
votes_fallback_time = '2017-12-17 00:00:00'

chiefs = {
    '0x8e2a84d6ade1e7fffee039a35ef5f19f13057152': '1.0',
    '0x9ef05f7f6deb616fd37ac3c959a2ddd25a54e4f5': '1.1',
    '0x0a3f6849f78076aefadf113f5bed87720274ddc0': '1.2'
}

mkr_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'

polls_address = '0xf9be8f0945acddeedaa64dfca5fe9629d0cf8e5d'
new_polls_address = '0xd3a9fe267852281a1e6307a1c37cdfd76d39b133'

votes_db = os.environ.get('VOTES_DB')
votes_scheduler = os.environ.get('VOTES_SCHEDULER')

staging_votes_db = os.environ.get('STAGING_VOTES_DB')
staging_votes_scheduler = os.environ.get('STAGING_VOTES_SCHEDULER')

# LIQUIDATIONS
dog_address = '0x135954d155898D42C90D2a57824C690e0c7BEf1B'
liquidations_fallback_block = 12316360
liquidations_fallback_time = '2021-04-26 14:02:08'

liquidations_db = os.environ.get('LIQUIDATIONS_DB')
absolute_import_path = '/opt/airflow/dags/abi/'
