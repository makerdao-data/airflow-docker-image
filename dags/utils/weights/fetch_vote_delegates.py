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
        FROM data_insights_cu.public.delegates;
    """
    ).fetchall():

        vote_delegates.append(vote_delegate[0])

    q = f"""
        SELECT block_number, FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", block_timestamp, "UTC"), topics, lower(address)
        FROM `bigquery-public-data.crypto_ethereum.logs`
        WHERE DATE(block_timestamp) >= "{start_date}"
        AND DATE(block_timestamp) <= "{end_date}"
        AND topics[safe_offset(0)] in ("0x2187b96b95fffefab01016c852705bc8ec76d1ea17dd5bffef25fd7136633644")
        AND lower(address) = lower("0xd897f108670903d1d6070fcf818f9db3615af272") ;
    """

    records = list()
    for block, timestamp, topics, address in bq_query(q):

        vote_delegate = topics[2][:2] + topics[2][-40:]
        delegate = topics[1][:2] + topics[1][-40:]

        if vote_delegate not in vote_delegates:

            r = [
                load_id,
                block,
                timestamp,
                address,
                vote_delegate,
                delegate,
                None,
                None
            ]

            records.append(r)


    if records:
        pattern = _write_to_stage(
            sf, records, "data_insights_cu.public.extracts"
        )
        if pattern:
            _write_to_table(
                sf,
                "data_insights_cu.public.extracts",
                "data_insights_cu.public.delegates",
                pattern,
            )
            _clear_stage(sf, "data_insights_cu.public.extracts", pattern)

        _update_delegates()