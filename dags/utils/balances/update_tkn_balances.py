import pandas as pd
from pandas import DataFrame
from snowflake.connector.pandas_tools import write_pandas

def _gen_bal_df(df: DataFrame) -> DataFrame:
    """
    Process transfer history and create holder balance dataframe
    """

    # Iterate through transfer history list and calculate balances
    bals = {}
    for tx in df:
        bals[tx[0]] = -abs(tx[2])
        bals[tx[1]] = tx[2]
    for pair in list(bals.items()):
        if pair[1] <= 0:
            val = abs(pair[1]) * 10**-18
            if val < 10**-18:
                del(bals[pair[0]])
            else:
                bals[pair[0]] = val

    # Formatting
    push_df = pd.DataFrame.from_dict(bals, orient='index').reset_index()
    push_df.rename(columns={'index': 'ADDRESS', 0: 'BALANCE'}, inplace=True)

    # Return dataframe
    return push_df


def update_token_balances(tkn: str, conn) -> None:
    """
    Update a given tokens holder balance table
    """

    # Verifying token is supported
    if tkn.upper() not in ["MKR", "DAI"]:
        raise AirflowFailException(f"Invalid token selection: {tkn}")

    # Obtaining transfer history
    df = conn.cursor().execute(f"SELECT SENDER, RECEIVER, AMOUNT FROM MAKER.HISTORY.{tkn}_TRANSFERS").fetchall()

    # Process transfer history and create balance dataframe
    push_df = _gen_bal_df(df)

    # Clear table then push balances
    conn.cursor().execute(f"DELETE FROM MAKER.BALANCES.{tkn}_BALANCES")
    write_pandas(conn, push_df, f'{tkn}_BALANCES', database='MAKER', schema='BALANCES')

    # Return None
    return
