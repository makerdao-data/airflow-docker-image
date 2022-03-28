import requests
from bs4 import BeautifulSoup
from dags.connectors.chain import chain
from dags.connectors.sf import sf
from dags.utils.weights.get_delegate_details import _get_delegate_details


def _update_delegates():
    for vote_delegate, type, name in sf.execute("""
        SELECT vote_delegate, type, name
        FROM delegates.public.delegates
        WHERE name IS NULL;
    """
    ).fetchall():

        if not type and not name:
            
            name, type = _get_delegate_details(chain, vote_delegate)

            if not name:
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
            else:
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}', name = '{name}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
        elif type and not name:

            name, type = _get_delegate_details(chain, vote_delegate)

            if name:
                
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}', name = '{name}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
        
        else:
            
            pass
    
    # Update start_date
    start_dates = dict()

    URL = f"""https://github.com/makerdao/community/blob/master/governance/delegates/"""
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")

    rows = soup.find_all(role="rowheader")

    for r in rows:

        title = r.find("span").text
        if title[:2] == '0x':

            URL = f"""https://github.com/makerdao/community/blob/master/governance/delegates/{title}/metrics.md"""
            page = requests.get(URL)
            soup = BeautifulSoup(page.content, "html.parser")
            
            start_date = soup.find(id="readme").find("article").find("table").find_all("td")[4].find("div").text
            start_dates[title.lower()] = start_date[:10] + ' 00:00:00'

    delegates_current = sf.execute(f"""
        select vote_delegate
        from delegates.public.delegates
        where start_date is null
            and vote_delegate in {tuple(start_dates.keys())};
    """).fetchall()

    for vote_delegate in delegates_current:

        sf.execute(f"""
            UPDATE delegates.public.delegates
            SET start_date = '{start_dates[vote_delegate[0].lower()]}'
            WHERE vote_delegate = '{vote_delegate[0]}';
        """)

    return
