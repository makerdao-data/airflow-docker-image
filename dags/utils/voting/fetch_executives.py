import requests
from bs4 import BeautifulSoup
from dags.connectors.sf import sf


def _fetch_executives(**setup):

    sf_execs = sf.execute(
        f"""
        SELECT code, title
        FROM {setup['votes_db']}.internal.yays
        WHERE type = 'executive'; """
    ).fetchall()

    codes = [executive[0] for executive in sf_execs]
    # adding addresses to omit (old DEFCON5 Disable the Liquidation Freeze spell and Activate DSChief v1.2)
    codes = codes + [
        '0x0000000000000000000000000000000000000000',
        '0x02fc38369890aff2ec94b28863ae0dacdb2dbae3',
    ]

    titles = [executive[1] for executive in sf_execs]

    url = f"https://api.github.com/repos/makerdao/community/contents/governance/votes"
    r = requests.get(url)
    res = r.json()

    records = list()
    for r in res:
        if r['type'] == 'file':

            page = requests.get(r['html_url'])
            soup = BeautifulSoup(page.content, "html.parser")
            entry = soup.find(id="readme").find("article").find("table").find("tbody").find("tr").find_all("td")
            
            if len(entry) >= 4:

                unresolved_title = entry[0].find("div").text
                code = entry[3].find("div").text
                code = code.lower()

                if len(code) == 42:

                    if "Template - [Executive Vote] " in unresolved_title:
                        resolved = unresolved_title.replace("Template - [Executive Vote] ", "")
                    else:
                        resolved = unresolved_title

                    if code not in codes:
                        if resolved not in titles:
                            records = [
                                [
                                    'executive',
                                    code,
                                    resolved,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                ]
                            ]


    return records
