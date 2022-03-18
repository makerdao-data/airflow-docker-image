import json
import os
from datetime import datetime

import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from web3 import Web3

import pandas as pd
import plotly
from dotenv import load_dotenv
from plotly import graph_objs as go

from ..connectors.chain import chain

# On-chain VAT data interaction
vat_abi = """
            [{"constant":true,"inputs":[],"name":"debt",
            "outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
            "payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[],"name":"Line",
            "outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
            "payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],
            "name":"ilks","outputs":[{"internalType":"uint256","name":"Art","type":"uint256"},
            {"internalType":"uint256","name":"rate","type":"uint256"},
            {"internalType":"uint256","name":"spot","type":"uint256"},
            {"internalType":"uint256","name":"line","type":"uint256"},
            {"internalType":"uint256","name":"dust","type":"uint256"}],
            "payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[{"name":"address","type":"address"}],
            "name":"dai","outputs":[{"name":"dai","type":"uint256"}],
            "payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[{"name":"address","type":"address"}],
            "name":"sin","outputs":[{"name":"sin","type":"uint256"}],
            "payable":false,"stateMutability":"view","type":"function"}]
          """


def get_vat_data():

    try:
        vat = chain.eth.contract(
            address="0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B", abi=vat_abi)
        Line = vat.functions.Line().call() / 10**45

    except Exception as e:
        print(e)
        Line = 0

    return Line


# Query executor
def async_queries(sf, all_queries) -> dict:

    started_queries = []
    for i in all_queries:
        sf.execute(i["query"])
        started_queries.append(dict(qid=sf.sfqid, id=i["id"]))

    all_results = {}
    limit = len(started_queries)
    control = 0

    while limit != control:

        for i in started_queries:

            if i["id"] not in all_results.keys():

                try:

                    check_results = sf.execute(f"""
                        SELECT *
                        FROM table(result_scan('{i["qid"]}'))
                        """)

                    if isinstance(check_results, SnowflakeCursor):
                        df = check_results.fetch_pandas_all()
                        result = df.where(pd.notnull(df), None).values.tolist()
                        all_results[i["id"]] = result

                except Exception as e:
                    print(str(e))

        control = len(all_results.keys())

    return all_results


# Bar graph generator
def main_bar(data) -> dict:

    df = pd.DataFrame(data, columns=['TYPE', 'VALUE_LOCKED', 'DEBT'])
    data = []
    data.append(
        go.Bar(x=df['TYPE'].tolist(),
               y=df['VALUE_LOCKED'].tolist(),
               name='Value locked (USD)',
               marker_color='#dddddd'))

    data.append(
        go.Bar(x=df['TYPE'].tolist(),
               y=df['DEBT'].tolist(),
               name='Debt (DAI)',
               marker_color='#1aab9b'))

    # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    layout = go.Layout(title={
        "text": "Value locked (USD) & debt (DAI) split by vault type",
        "x": 0.5,
        "xanchor": "center"
    },
                       barmode='group',
                       xaxis_tickangle=-45,
                       hovermode="x unified",
                       paper_bgcolor="#fcfcfc",
                       plot_bgcolor="#fcfcfc")

    fig = go.Figure(data=data, layout=layout)
    fig.update_traces(hovertemplate="%{y:,.2f}<extra></extra>")

    graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graph_json


# Pie graph generator
def main_pie(data) -> dict:

    colors = ['#1aab9b', '#dddddd', '#444444', '#f0f0f0']

    labels = []
    values = []

    for label, value in data:

        labels.append(label)
        values.append(value)

    # Use `hole` to create a donut-like pie chart
    fig = go.Figure(data=[
        go.Pie(
            labels=labels,
            values=values,
            hole=.3,
            hovertemplate='Value locked (USD): %{value:,.2f}<extra></extra>')
    ],
                    layout=go.Layout(hovermode='x unified'))

    fig.update_traces(marker=dict(colors=colors))
    fig.update_layout(dict(hovermode="x unified"))

    graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graph_json


# URL formatter
def link(item, url, title=None) -> str:
    if title:
        title = f"""title={title}"""
    else:
        title = ""
    return f"""<a {title} href="{url}">{item}</a>"""


# Obtain main page data
def update_vault_data(sf):
    # test snowflake connection and reconnect if necessary
    try:
        if sf.is_closed():
            sf = sf_connect()
        if sf.is_closed():
            raise Exception("Reconnection failed")

    except Exception as e:
        print(e)
        return dict(status="failure", data="Database connection error")

    try:
        # list of available collaterals
        ilks_query = 'SELECT ilk FROM internal.ilks; '

        # current vaults list
        vaults_query = \
            """
                SELECT
                    vault,
                    ilk,
                    collateral,
                    debt,
                    available_debt,
                    available_collateral,
                    owner,
                    collateralization,
                    osm_price
                FROM mcd.public.current_vaults;
                """

        pie_query = \
            """
                select
                case
                when SPLIT_PART(v.ilk, '-', 0) = 'PSM' then 'STABLE'
                when i.type = 'lp' then 'NON-STABLE'
                when v.ilk like 'GUNIV3%' then 'STABLE'
                when i.type = 'stablecoin' then 'STABLE'
                when i.type = 'coin' then 'NON-STABLE'
                when SPLIT_PART(v.ilk, '-', 0) = 'DIRECT' then 'NON-STABLE'
                when i.type = 'None' = 'NON-STABLE'
                else upper(i.type)
                end type,
                sum( v.collateral * v.osm_price ) value_locked
                from mcd.public.current_vaults v
                join mcd.internal.ilks i
                on v.ilk = i.ilk
                where v.ilk != 'SAI'
                group by i.type, v.ilk;
            """

        bar_query = \
            """
                select type, case when type = 'RWA' then null else sum(value_locked) end VALUE_LOCKED, sum(debt)
                from (
                select
                case
                when SPLIT_PART(v.ilk, '-', 0) = 'PSM' then 'PSM'
                when v.ilk like 'RWA0%' then 'RWA'
                when SPLIT_PART(v.ilk, '-', 0) = 'DIRECT' then 'D3M'
                when v.ilk like 'GUNIV3%' then 'G-UNI'
                else upper('REGULAR VAULT')
                end type,
                v.ilk,
                sum( v.collateral * v.osm_price ) value_locked, sum( v.debt ) debt
                from mcd.public.current_vaults v
                join mcd.internal.ilks i
                on v.ilk = i.ilk
                group by i.type, v.ilk
                )
                where debt > 0
                group by type;
            """

        sin_query = \
            """
                select sin
                from mcd.internal.vow
                order by timestamp desc
                limit 1;
            """

        all_queries = [
            dict(query=vaults_query, id='vaults'),
            dict(query=ilks_query, id='ilks'),
            dict(query=pie_query, id='pie'),
            dict(query=bar_query, id='bar'),
            dict(query=sin_query, id='sin')
        ]

        sf_responses = async_queries(sf, all_queries)

        vaults = sf_responses['vaults']
        ilks = sf_responses['ilks']
        pie_data = sf_responses['pie']
        bar_data = sf_responses['bar']
        sin = sf_responses['sin']
        sin = sin[0][0]

        # data processing
        collaterals = dict()
        for m in ilks:
            collaterals[m[0]] = dict()

        Line = get_vat_data()

        owners = set()
        active_owners = set()

        for c in collaterals:
            collaterals[c]['debt'] = 0
            collaterals[c]['locked_amount'] = 0
            collaterals[c]['available_debt'] = 0
            collaterals[c]['available_collateral'] = 0
            collaterals[c]['available_collateral_usd'] = 0
            collaterals[c]['vaults_num'] = 0
            collaterals[c]['active_num'] = 0
            collaterals[c]['osm_price'] = 0

        # iterate over all vaults
        for vault in vaults:

            # build sets of owners and active owners
            owners.add(vault[6])
            if vault[3] > 20:
                active_owners.add(vault[6])

            collaterals[vault[1]]['vaults_num'] += 1
            collaterals[vault[1]]['active_num'] += (1 if vault[3] > 20 else 0)
            collaterals[vault[1]]['locked_amount'] += vault[2]
            collaterals[vault[1]]['debt'] += vault[3]
            collaterals[vault[1]]['available_debt'] += vault[4] or 0
            collaterals[vault[1]]['available_collateral'] += vault[5]
            if vault[8]:
                collaterals[vault[1]]['osm_price'] = vault[8]
            collaterals[vault[1]]['available_collateral_usd'] += vault[5] \
                * collaterals[vault[1]]['osm_price']

        # add additional information
        for c in collaterals:
            collaterals[c]['locked_value'] = collaterals[c][
                'locked_amount'] * collaterals[c]['osm_price']
            collaterals[c]['collateralization'] = \
                (collaterals[c]['locked_value'] / collaterals[c]['debt'
                 ] if collaterals[c]['locked_value']
                 and collaterals[c]['debt'] and collaterals[c]['debt']
                 > 1e-10 else None)

        collaterals_list = [[
            collateral,
            c['active_num'],
            c['vaults_num'],
            c['locked_value'],
            c['debt'],
            c['available_debt'],
            c['available_collateral'],
            c['available_collateral_usd'],
            c['collateralization'],
            c['osm_price'],
        ] for (collateral, c) in collaterals.items()]
        collaterals_list.sort(key=lambda _c: _c[4], reverse=True)

        # calculate total stats
        vaults_num = sum([c[2] for c in collaterals_list])
        active_num = sum([c[1] for c in collaterals_list])
        locked_value = sum([c[3] for c in collaterals_list])
        total_debt = sum([c[4] for c in collaterals_list])
        available_debt = sum([c[5] for c in collaterals_list])
        available_collateral = sum([c[7] for c in collaterals_list])

        collaterals_list = [[
            link(c[0], '/collateral/%s' % c[0], 'Vaults using %s' % c[0]),
            '{0:,d}'.format(c[1]),
            '{0:,d}'.format(c[2]),
            '{0:,.2f}'.format(c[3]),
            '{0:,.2f}'.format(c[4]),
            '{0:,.2f}'.format(c[5]),
            '{0:,.2f}'.format(c[6]),
            ('{0:,.2f}%'.format(100 * c[8]) if c[8] else '-'),
        ] for c in collaterals_list]

        if not sin:
            sin = 0

        total_debt = total_debt + sin

        collaterals_data = []
        for i in collaterals_list:
            collaterals_data.append(
                dict(
                    COLLATERAL=i[0],
                    ACTIVE_VAULTS=i[1],
                    TOTAL_VAULTS=i[2],
                    LOCKED_VALUE=i[3],
                    TOTAL_DEBT=i[4],
                    AVAILABLE_DEBT=i[5],
                    AVAILABLE_COLLATERAL=i[6],
                    COLLATERALIZATION=i[7],
                ))

        total_collateralization = ('{0:,.2f}%'.format(
            100 * locked_value / total_debt) if locked_value and total_debt
                                   and total_debt > 1e-10 else '-')
        debt_ceiling = '{0:,.0f}'.format(Line)
        debt_utilization = ('{0:,.2f}%'.format(100 * total_debt /
                                               Line) if Line else '')
        total_debt = '{0:,.2f}'.format(total_debt)
        locked_value = '{0:,.2f}'.format(locked_value)
        available_debt = \
            ('{0:,.2f}'.format(available_debt) if available_debt else '0')
        available_collateral = \
            ('{0:,.2f}'.format(available_collateral) if available_collateral else '0'
             )
        vaults_num = '{0:,d}'.format(vaults_num)
        active_num = ('{0:,d}'.format(active_num) if active_num else '0')
        owners_num = '{0:,d}'.format(len(owners))
        active_owners_num = '{0:,d}'.format(len(active_owners))

        pie = main_pie(pie_data)
        bar = main_bar(bar_data)
        sin = '{0:,.2f}'.format(sin)

        # collat_dict = {}
        # for i in range(len(collaterals_data)):
        #     collat_dict[collaterals_data[i]['COLLATERAL'].split('using')
        #                 [1].split(' ')[1]] = collaterals_data[i]

        sf.execute(
            f"""INSERT OVERWRITE INTO MCD.TRACKERS.VAULT_TRACKER(TOTAL_DEBT, COLLATERALS_NUM, VAULTS_NUM, ACTIVE_NUM, DEBT_CEILING, DEBT_UTILIZATION, AVAILABLE_DEBT, AVAILABLE_COLLATERAL, OWNERS, ACTIVE_OWNERS, COLLATERALIZATION, LOCKED_VALUE, REFRESH, SIN)
            VALUES ('{total_debt}', '{len(ilks)}', '{vaults_num}', '{active_num}', '{debt_ceiling}', '{debt_utilization}', '{available_debt}', '{available_collateral}', '{owners_num}', '{active_owners_num}', '{total_collateralization}', '{locked_value}', '{datetime.utcnow()}', '{sin}')"""
        )

        sf.execute(f"""UPDATE MCD.TRACKERS.VAULT_TRACKER
            SET COLLATERALS = PARSE_JSON($${dict({'data':collaterals_data})}$$),
            PIE = PARSE_JSON('{pie}'),
            BAR = PARSE_JSON('{bar}')
        """)

        return None
        # must properly jsonize collaterals_data

    except Exception as e:
        print(e)
        return dict(status="failure", data="Backend error: %s" % e)
