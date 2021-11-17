def transaction_write_to_table(conn, stage, table, pattern, purge=True):

    conn.execute(
        f"""COPY INTO {table} FROM @{stage}/{pattern}.gz FILE_FORMAT=(TYPE=CSV, FIELD_DELIMITER='|', NULL_IF='None') PURGE={purge}; """
    )

    return True


def merge_into_auction_table(conn, stage, table, pattern):

    conn.execute(
        f"""MERGE INTO {table} USING
    (
        select
            $1 LOAD_ID,
            $2 AUCTION_ID,
            $3 AUCTION_START,
            $4 VAULT,
            $5 ILK,
            $6 URN,
            $7 OWNER,
            $8 DEBT,
            $9 AVAILABLE_COLLATERAL,
            $10 PENALTY,
            $11 SOLD_COLLATERAL,
            $12 RECOVERED_DEBT,
            $13 ROUND,
            $14 AUCTION_END,
            $15 FINISHED
        from @{stage} (file_format => staging_mcd.staging.x_format, pattern=> '.*{pattern}.*')
    ) t ON {table}.ILK = t.ILK AND {table}.AUCTION_ID = t.AUCTION_ID
    WHEN
        MATCHED THEN
            UPDATE SET
                LOAD_ID = t.LOAD_ID,
                AUCTION_ID = t.AUCTION_ID,
                AUCTION_START = t.AUCTION_START,
                VAULT = t.VAULT,
                ILK = t.ILK,
                URN = t.URN,
                OWNER = t.OWNER,
                DEBT = t.DEBT,
                AVAILABLE_COLLATERAL = t.AVAILABLE_COLLATERAL,
                PENALTY = t.PENALTY,
                SOLD_COLLATERAL = t.SOLD_COLLATERAL,
                RECOVERED_DEBT = t.RECOVERED_DEBT,
                ROUND = t.ROUND,
                AUCTION_END = t.AUCTION_END,
                FINISHED = t.FINISHED
        WHEN NOT MATCHED THEN
            INSERT (LOAD_ID, AUCTION_ID, AUCTION_START, VAULT, ILK, URN, OWNER, DEBT, AVAILABLE_COLLATERAL, PENALTY, SOLD_COLLATERAL, RECOVERED_DEBT, ROUND, AUCTION_END, FINISHED)
            VALUES (t.LOAD_ID, t.AUCTION_ID, t.AUCTION_START, t.VAULT, t.ILK, t.URN, t.OWNER, t.DEBT, t.AVAILABLE_COLLATERAL, t.PENALTY, t.SOLD_COLLATERAL, t.RECOVERED_DEBT, t.ROUND, t.AUCTION_END, t.FINISHED); """
    )


def merge_into_round_table(conn, stage, table, pattern):

    conn.execute(
        f"""MERGE INTO {table} USING
    (
        select
            $1 LOAD_ID,
            $2 AUCTION_ID,
            $3 ROUND,
            $4 ROUND_START,
            $5 INITIAL_PRICE,
            $6 DEBT,
            $7 AVAILABLE_COLLATERAL,
            $8 SOLD_COLLATERAL,
            $9 RECOVERED_DEBT,
            $10 KEEPER,
            $11 INCENTIVES,
            $12 END_PRICE,
            $13 ROUND_END,
            $14 FINISHED,
            $15 ILK
        from @{stage} (file_format => staging_mcd.staging.x_format, pattern=> '.*{pattern}.*')
    ) t ON {table}.ILK = t.ILK AND {table}.AUCTION_ID = t.AUCTION_ID AND {table}.ROUND = t.ROUND
    WHEN
        MATCHED THEN
            UPDATE SET
                LOAD_ID = t.LOAD_ID,
                AUCTION_ID = t.AUCTION_ID,
                ROUND = t.ROUND,
                ROUND_START = t.ROUND_START,
                INITIAL_PRICE = t.INITIAL_PRICE,
                DEBT = t.DEBT,
                AVAILABLE_COLLATERAL = t.AVAILABLE_COLLATERAL,
                SOLD_COLLATERAL = t.SOLD_COLLATERAL,
                RECOVERED_DEBT = t.RECOVERED_DEBT,
                KEEPER = t.KEEPER,
                INCENTIVES = t.INCENTIVES,
                END_PRICE = t.END_PRICE,
                ROUND_END = t.ROUND_END,
                FINISHED = t.FINISHED,
                ILK = t.ILK
        WHEN NOT MATCHED THEN
            INSERT (LOAD_ID, AUCTION_ID, ROUND, ROUND_START, INITIAL_PRICE, DEBT, AVAILABLE_COLLATERAL, SOLD_COLLATERAL, RECOVERED_DEBT, KEEPER, INCENTIVES, END_PRICE, ROUND_END, FINISHED, ILK)
            VALUES (t.LOAD_ID, t.AUCTION_ID, t.ROUND, t.ROUND_START, t.INITIAL_PRICE, t.DEBT, t.AVAILABLE_COLLATERAL, t.SOLD_COLLATERAL, t.RECOVERED_DEBT, t.KEEPER, t.INCENTIVES, t.END_PRICE, t.ROUND_END, t.FINISHED, t.ILK); """
    )


def write_barks_to_table(conn, stage, table, pattern):

    q = f"""COPY INTO {table} (LOAD_ID, BLOCK, TIMESTAMP, TX_HASH, URN, VAULT, ILK, OWNER, COLLATERAL, DEBT, PENALTY, RATIO, OSM_PRICE, MKT_PRICE, AUCTION_ID, CALLER, KEEPER, GAS_USED, STATUS, REVERT_REASON)
        FROM @{stage} FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PATTERN='.*{pattern}.*'; """

    conn.execute(q)


def write_actions_to_table(conn, stage, table, pattern):

    q = f"""COPY INTO {table} (LOAD_ID, AUCTION_ID, TIMESTAMP, BLOCK, TX_HASH, TYPE, CALLER, DATA, DEBT, INIT_PRICE, MAX_COLLATERAL_AMT, AVAILABLE_COLLATERAL, SOLD_COLLATERAL, MAX_PRICE, COLLATERAL_PRICE, OSM_PRICE, RECOVERED_DEBT, CLOSING_TAKE, KEEPER, INCENTIVES, URN, GAS_USED, STATUS, REVERT_REASON, ROUND, BREADCRUMB, ILK, MKT_PRICE)
        FROM @{stage} FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PATTERN='.*{pattern}.*'; """

    conn.execute(q)
