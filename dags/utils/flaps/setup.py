from datetime import datetime
from dags.connectors.sf import sf


def _setup():

    fallback_block = 8928151
    load_id = datetime.utcnow().__str__()[:19]

    start_block = sf.execute(
        f"""
            SELECT MAX(end_block)
            FROM maker.scheduler.flaps;
        """
    ).fetchone()

    if not start_block[0]:
        start_block = fallback_block
    else:
        start_block = start_block[0]

    end_block = sf.execute(
        f"""
            SELECT MAX(block)
            FROM edw_share.raw.events;
        """
    ).fetchone()

    if not end_block:
        end_block = 0
    else:
        end_block = end_block[0]

    setup = {
        'load_id': load_id,
        'start_block': start_block,
        'end_block': end_block,
    }

    return setup
