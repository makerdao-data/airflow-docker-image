import sys
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/')

from airflow.decorators import dag, task
from dags.connectors.gsheets import gclient
from dags.connectors.sf import sf
from dags.utils.reports.growth_reports.kpis import growth_report_updater
from dags.utils.reports.votes import populate_vote_sheet


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": "airflow@data.makerdao.network",
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 4, 4),
    max_active_runs=1,
    catchup=False,
)
def daily_reports():

    @task()
    def update_growth_report():

        growth_report_updater(sf, gclient)

        return
    
    @task()
    def update_vote_sheet():
        populate_vote_sheet(
            gclient.open_by_url(
                'https://docs.google.com/spreadsheets/d/1MkC0GILLWaRUEKU1hz6nKft21uyZATTreAVxg5x3hkU/'
            )
        )

    update_vote_sheet()
    update_growth_report()


daily_reports = daily_reports()
