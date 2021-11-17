from dags.utils.voting.tooling.get_polls import _get_polls_data
from dags.connectors.sf import sf


def _fetch_api_polls(**setup):

    max_poll = (
        sf.execute(
            f"""
        SELECT max(cast(code as integer))
        FROM {setup['votes_db']}.internal.yays
        WHERE type = 'poll'; """
        ).fetchone()[0]
        or 0
    )

    # Load new Polls from the Governance Portal API
    polls = _get_polls_data(max_poll, setup['end_time'])

    records = [
        ['poll', poll[0], poll[1], poll[2], poll[3], poll[4], poll[5], poll[6], poll[7]]
        for poll in polls
        if poll[0] > max_poll
    ]

    print(f"API POLLS: {len(polls)} read, {len(records)} prepared to upload")

    return records
