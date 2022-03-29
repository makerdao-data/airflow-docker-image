import sys
sys.path.append('/opt/airflow/')
from dags.connectors.gcp import bq_query
from dags.connectors.sf import sf
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage
from dags.utils.weights.update_delegates import _update_delegates


def _fetch_vote_delegates(start_date, end_date, load_id):

    vote_delegates = list()
    for vote_delegate in sf.execute("""
        SELECT vote_delegate
        FROM delegates.public.delegates;
    """
    ).fetchall():

        vote_delegates.append(vote_delegate[0])

    delegates = sf.execute(f"""
        SELECT block, timestamp, topic1, topic2, contract, tx_hash
        FROM "EDW_SHARE"."RAW"."EVENTS"
        WHERE DATE(timestamp) >= '{start_date}'
        AND DATE(timestamp) <= '{end_date}'
        AND topic0 = '0x2187b96b95fffefab01016c852705bc8ec76d1ea17dd5bffef25fd7136633644'
        AND contract = '0xd897f108670903d1d6070fcf818f9db3615af272';
    """).fetchall()

    records = list()
    for block, timestamp, topic1, topic2, address, tx_hash in delegates:

        vote_delegate = topic2[:2] + topic2[-40:]
        delegate = topic1[:2] + topic1[-40:]

        if vote_delegate not in vote_delegates:

            r = [
                load_id,
                block,
                timestamp,
                address,
                vote_delegate,
                delegate,
                None,
                None,
                None,
                tx_hash
            ]

            records.append(r)

    if records:
        pattern = _write_to_stage(
            sf, records, "delegates.public.extracts"
        )
        if pattern:
            _write_to_table(
                sf,
                "delegates.public.extracts",
                "delegates.public.delegates",
                pattern,
            )
            _clear_stage(sf, "delegates.public.extracts", pattern)

        _update_delegates()