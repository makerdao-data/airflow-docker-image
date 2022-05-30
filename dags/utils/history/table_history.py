from airflow.exceptions import AirflowFailException


def update_table_history(sf):
    """
    Function to update table of recent table updates.

    This could be replaced with two queries:
        1) query to fetch recent table modifications
        2) query to overwrite modification history table

    This should probably be rewritten some time with more optimal querying.
    However the overhead found in the following code is negligible.
    """

    results = []

    try:
        for schema in ['MAKER', 'DELEGATES', 'LIQUIDATIONS', 'MCD']:
            results.append(
                sf.execute(
                    f"""SELECT CONCAT("TABLE_CATALOG",'.',"TABLE_SCHEMA",'.',"TABLE_NAME") AS TABLE_NAME, "LAST_ALTERED" FROM {schema}.INFORMATION_SCHEMA."TABLES" WHERE "TABLE_OWNER" IS NOT NULL"""
                ).fetchall())
    except Exception as e:
        raise AirflowFailException(e)

    result = sum(results, [])

    try:
        sf.execute("delete from UTIL_DB.PUBLIC.TABLE_UPDATES")
        sf.executemany(
            "INSERT INTO UTIL_DB.PUBLIC.TABLE_UPDATES VALUES (%s, %s)", result)
    except Exception as e:
        raise AirflowFailException(e)
