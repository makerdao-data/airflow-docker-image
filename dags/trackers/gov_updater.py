import json
import os

import requests
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from airflow.exceptions import AirflowFailException

import pandas as pd
from dotenv import load_dotenv


# Query execution
def exec_queries(sf, all_queries):
    all_results = {}
    try:
        for i in all_queries:
            result = sf.execute(i["query"]).fetchall()
            all_results[i["id"]] = result
    except Exception as e:
        print(e)
    return all_results


# Get all poll information
def get_all_polls(base_link="https://governance-portal-v2.now.sh"):

    response = requests.get(base_link + "/api/polling/all-polls")
    if response.status_code != 200:
        # something went wrong...
        print("API error {}.".format(response.status_code))
        polls = dict()
    else:
        content = response.json()["polls"]
        polls = {
            str(poll["pollId"]): (poll["title"], poll["options"])
            for poll in content
        }

    return polls


# URL formatter
def link(item, url, title=None) -> str:
    if title:
        title = f"""title={title}"""
    else:
        title = ""
    return f"""<a {title} href="{url}">{item}</a>"""


# HTML formatter
def html_table(
    content,
    widths=None,
    table_class="simple-table",
    table_id="sorted-table",
    expose=None,
    tooltip=True,
):

    if len(content) > 1:

        html = ("<table " +
                ("class='%s'" % table_class if table_class else "") +
                ("id='%s'" % table_id if table_id else "") + ">")
        html += "<thead><tr>"
        for i, column in enumerate(content[0]):
            if widths and len(widths) > i:
                width = widths[i]
            else:
                width = "auto"
            html += "<th width='%s'>" % width + str(column) + "</th>"
        html += "</tr></thead>"

        html += "<tbody>"
        for row_num, content_row in enumerate(content[1:]):
            html += ("<tr%s>" % ' class="row_expose"'
                     if expose and row_num in expose else "")
            for i, content_item in enumerate(content_row):
                if tooltip and i == len(content_row) - 1:
                    html += ('<td title="%s">' % str(content_item) +
                             str(content_item) + "</td>")
                else:
                    html += ("<td%s>" % (' class="row_expose"' if expose
                                         and row_num in expose else "") +
                             str(content_item) + "</td>")
            html += "</tr>"
        html += "</tbody>"

        html += "</table>"
    else:
        html = ""

    return html


def update_gov_data(sf) -> None:

    try:

        voters_query = f"""
            select o.voter, '', o.stake, o.yay1, o.yay2, o.yay3, o.yay4, o.yay5, o.since, o.last_voting, d.name
            FROM (SELECT distinct case when p.from_address is null then v.voter else p.from_address end voter, '', v.stake, v.yay1, v.yay2, v.yay3, v.yay4, v.yay5, v.since, v.last_voting
            FROM {os.getenv("MCDGOV_DB", "mcd")}.public.current_voters v
            LEFT JOIN {os.getenv("MCDGOV_DB", "mcd")}.internal.vote_proxies p
            on v.voter = p.proxy
            ORDER BY v.stake desc) o
            LEFT JOIN delegates.public.delegates d
            on o.voter = d.vote_delegate;
        """

        yays_query = f"""
            SELECT yay, option, first_voting, approval, voters, first_voting, type
            FROM
                (SELECT 'executive' as type, yay, null as option, round(sum(dapproval), 6) as approval, count(distinct voter) as voters, min(timestamp) as first_voting
                FROM {os.getenv("MCDGOV_DB", "mcd")}.public.votes
                WHERE operation not in ('CHOOSE', 'FINAL_CHOICE', 'RETRACT', 'CREATE_PROXY', 'BREAK_PROXY')
                GROUP BY yay
                UNION
                SELECT 'poll' as type, yay, option, round(sum(dapproval), 6) as approval, count(distinct voter) as voters, min(timestamp) as first_voting
                FROM {os.getenv("MCDGOV_DB", "mcd")}.public.votes
                WHERE operation in ('CHOOSE', 'RETRACT')
                GROUP BY yay, option);
            """

        titles_query = f"""
            SELECT code, title
            FROM {os.getenv("MCDGOV_DB", "mcd")}.internal.yays;
            """

        hat_query = f"""
            SELECT hat as hat
            FROM {os.getenv("MCDGOV_DB", "mcd")}.public.votes
            WHERE operation != 'FINAL_CHOICE'
            ORDER BY order_index desc
            LIMIT 1;
            """

        active_polls_query = f"""
            SELECT code
            FROM {os.getenv("MCDGOV_DB", "mcd")}.internal.yays
            WHERE type = 'poll' and end_timestamp > (SELECT max(load_id) FROM {os.getenv("MCDGOV_DB", "mcd")}.internal.votes_scheduler);
        """

        all_queries = [
            dict(query=voters_query, id="voters"),
            dict(query=yays_query, id="yays"),
            dict(query=titles_query, id="titles"),
            dict(query=hat_query, id="hat"),
            dict(query=active_polls_query, id="active_polls"),
        ]

        # snowflake data ingestion

        sf_responses = exec_queries(sf, all_queries)
        voters = sf_responses["voters"]
        yays = sf_responses["yays"]
        titles = sf_responses["titles"]
        hat = sf_responses["hat"][0][0]
        active_polls_repsonse = sf_responses["active_polls"]

        active_polls = list()
        [active_polls.append(code[0]) for code in active_polls_repsonse]

        polls_metadata = get_all_polls()

        titles = {y[0]: y[1] for y in titles}
        titles[
            "0x0000000000000000000000000000000000000000"] = "Activate DSChief v1.2"

        staked = active = 0
        last_vote = None
        for v in voters:
            staked += v[2]
            if round(v[2], 6) > 0:
                active += 1
            if not last_vote or v[9] > last_vote:
                last_vote = v[9]

        voters_list = [[
            link(v[10] if v[10] else v[0], f"/address/{ v[0]}",
                 v[10] if v[10] else v[0]),
            "{0:,.2f}".format(v[2] or 0),
            "<br>".join([
                link(titles.get(y, y), f"/yay/{y}", titles.get(y, y))
                for y in v[3:8] if y
            ]),
            v[8].date(),
            v[9] or "",
        ] for v in voters]

        # prepare output data
        voters_data = []
        for voter in voters_list:
            voters_data.append(
                dict(
                    VOTER=voter[0],
                    STAKE=voter[1],
                    CURRENT_VOTES=voter[2],
                    SINCE=voter[3].strftime("%Y-%m-%d"),
                    LAST=voter[4].strftime("%Y-%m-%d %H:%M:%S"),
                ))

        executives = [y for y in yays if y[6] == "executive"]

        expose = [
            i for i in range(len(executives))
            if executives[i][0] == hat and executives[i][0] is not None
        ]

        executives_list = [[
            link(y[0], f"/yay/{y[0]}"),
            y[5].date(),
            titles.get(y[0], y[0]),
            "{0:,.2f}".format(y[3] or 0),
            y[4] or 0,
        ] for y in executives]
        executives_list = [[
            "Spell", "Since", "Description", "Approval<br>(MKR)",
            "Voters<br>(#)"
        ]] + executives_list

        yays_table = html_table(
            executives_list,
            table_id="executives-table",
            widths=["40px", "55px", None, "55px", "45px"],
            expose=expose,
            tooltip=False,
        )

        polls_records = [y for y in yays if y[6] == "poll"]
        polls_votes = []
        polls = dict()
        for poll in polls_records:

            if poll[0] not in polls_votes:
                polls_votes.append(str(poll[0]))

            if poll[0] not in polls:
                polls[poll[0]] = dict(votes=0,
                                      stake=0,
                                      max_stake=0,
                                      winning=None,
                                      since=poll[2])

            polls[poll[0]]["votes"] += poll[4]
            polls[poll[0]]["stake"] += poll[3]
            if not polls[poll[0]]["winning"] or poll[3] > polls[
                    poll[0]]["max_stake"]:
                polls[poll[0]]["winning"] = poll[1]
                polls[poll[0]]["max_stake"] = poll[3]

        polls = [[key, *value.values()] for key, value in polls.items()]

        polls.sort(key=lambda x: int(x[0]) if x[0].isnumeric() else 0,
                   reverse=True)

        polls_list = [[
            link(p[0], f"/poll/{p[0]}"),
            p[5].date(),
            polls_metadata[p[0]][0],
            polls_metadata[p[0]][1].get(p[4], "Unknown"),
            1 if p[0] in active_polls else 0,
        ] for p in polls if p[0] in polls_metadata]

        polls_no_votes = sf.execute(f"""
            SELECT code, start_timestamp, title, 'Unknown', 1
            FROM {os.getenv("MCDGOV_DB", "mcd")}.internal.yays
            WHERE type = 'poll'
                and block_ended is null
                and code not in {tuple(polls_votes)};
            """).fetchall()

        for p in polls_no_votes:
            polls_list.append([
                link(p[0], f"/poll/{p[0]}"),
                p[1].date(),
                polls_metadata[p[0]][0],
                "Unknown",
                1,
            ])

        sorted(polls_list, key=lambda x: x[0])

        polls_list = [[
            "Poll", "Since", "Description", "Winning<br>option", "Active"
        ]] + polls_list

        polls_table = html_table(
            polls_list,
            table_id="polls-table",
            widths=["40px", "55px", None, "130px"],
            tooltip=False,
        )

        sf.execute(
            f"""INSERT OVERWRITE INTO MCD.TRACKERS.GOV_TRACKER(STAKED, ACTIVE, LAST_VOTE, VOTERS_NUM, YAYS, YAYS_NUM, POLLS, POLLS_NUM)
            VALUES ('{"{0:,.2f}".format(staked)}', '{"{0:,.0f}".format(active)}', '{last_vote.strftime("%Y-%m-%d %H:%M:%S")}', '{len(voters)}', $${yays_table}$$, '{len(executives)}', $${polls_table}$$, '{len(polls)}')"""
        )

        sf.execute(f"""UPDATE MCD.TRACKERS.GOV_TRACKER
            SET VOTERS = PARSE_JSON($${dict({'data': voters_data})}$$)
        """)

        return None

    except Exception as e:
        raise AirflowFailException(e)
