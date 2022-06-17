from datetime import datetime
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, _write_to_stage, _write_to_table, _clear_stage


def _snowflake_push():

    delegates = dict()
    for vote_delegate, date_timestamp, type, name in sf.execute(f"""
            SELECT vote_delegate, date(timestamp), type, name
            FROM delegates.public.delegates;
        """).fetchall():

        delegates[vote_delegate.lower()] = dict(
            start_date=date_timestamp,
            type=type,
            name=name
        )

    last_load = sf.execute(f"""
        select max(eod)
        from delegates.public.support;
    """).fetchone()[0]

    # fake last load (date of creation of first delegate -1 day)
    if not last_load:
        last_load = '2021-07-08'


    records = list()
    load_id = datetime.now().__str__()[:19]


    for eod, weight, vote_delegate in sf.execute(f"""
        SELECT to_varchar(p.eod::date) as eod, p.balance as weight, p.vote_delegate
        FROM delegates.public.power p
        WHERE eod > '{last_load}'
        ORDER BY eod;
    """).fetchall():

        type = None
        name = vote_delegate

        if vote_delegate in delegates:

            type = delegates[vote_delegate]['type']

            if delegates[vote_delegate]['start_date']:
                if delegates[vote_delegate]['start_date'].__str__()[:10] <= eod and type == 'recognized':

                    name = delegates[vote_delegate]['name']
        
        records.append([
            load_id,
            eod,
            type,
            vote_delegate,
            name,
            weight
        ])

    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"delegates.public.extracts")

        _write_to_table(
            sf,
            f"delegates.public.extracts",
            f"delegates.public.support",
            pattern,
        )
        _clear_stage(sf, f"delegates.public.extracts", pattern)