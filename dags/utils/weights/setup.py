from datetime import datetime, timedelta
from googleapiclient import discovery
from google.oauth2 import service_account
from dags.connectors.sf import sf


def _setup():

    FALLBACK_BLOCK = 12781698
    FALLBACK_DATE = '2021-07-07'

    last_date = sf.execute(f"""
        SELECT MAX(eod)
        FROM delegates.public.power;
    """
    ).fetchone()[0]

    if not last_date:
        last_date = FALLBACK_DATE
        last_date = datetime.strptime(last_date, "%Y-%m-%d").date() - timedelta(days=1)
    
    yesterday = datetime.utcnow().date() - timedelta(days=1)

    days = list()
    if last_date != yesterday:
        delta = yesterday - last_date
        
        for i in range(1, delta.days + 1):
            days.append(last_date + timedelta(days=i))

    return days
