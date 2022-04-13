from dags.connectors.sf import sf


def _setup():

    check = sf.execute(f"""
        SELECT ID
        FROM MAKER.RISK.VAULTS
        WHERE ID = 1;
    """).fetchone()

    if check in (None, (None,)):
        sf.execute("""
            INSERT INTO MAKER.RISK.VAULTS (ID)
            VALUES (1);
        """)

    start_block = sf.execute(f"""
        SELECT last_scanned_block
        FROM MAKER.RISK.VAULTS
        WHERE ID = 1;
    """).fetchone()

    if start_block in (None, (None,)):
        start_block = 8899999
    else:
        start_block = start_block[0]

    tip = sf.execute(f"""
        SELECT MAX(BLOCK), MAX(TIMESTAMP)
        FROM edw_share.raw.storage_diffs;
    """).fetchone()

    last_scanned_block = tip[0]
    latest_timestamp = tip[1]

    return start_block, last_scanned_block, latest_timestamp
