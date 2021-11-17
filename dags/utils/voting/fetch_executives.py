from dags.connectors.sf import sf
from dags.utils.voting.tooling.get_executives import get_execs


def _fetch_executives(**setup):

    executives = get_execs()

    sf_execs = sf.execute(
        f"""
        SELECT code
        FROM {setup['votes_db']}.internal.yays
        WHERE type = 'executive'; """
    ).fetchall()

    sf_execs = [executive[0] for executive in sf_execs]
    # adding addresses to omit (old DEFCON5 Disable the Liquidation Freeze spell and Activate DSChief v1.2)
    sf_execs = sf_execs + [
        '0x0000000000000000000000000000000000000000',
        '0x02fc38369890aff2ec94b28863ae0dacdb2dbae3',
    ]

    records = [
        [
            'executive',
            executive['address'].lower().strip(),
            executive['title'],
            None,
            None,
            None,
            None,
            None,
            None,
        ]
        for executive in executives
        if executive['address'].lower().strip() not in sf_execs
    ]
    if len(records) > 0:

        _execs = str()
        counter = 0
        for i in records:
            _execs = _execs + str(i[1]) + ', '
            counter += 1

    print(f"Executives titles: {len(executives)} read, {len(records)} prepared to upload")

    return records
