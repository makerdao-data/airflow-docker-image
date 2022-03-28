import requests
from bs4 import BeautifulSoup


def _get_delegate_name(page):
    type = 'recognized'
    soup = BeautifulSoup(page.content, "html.parser")
    name = soup.find(id="readme").find("article").find("table").find("td").find("div").text

    return type, name


def _get_delegate_details(chain, address):

    checksummed_address = chain.toChecksumAddress(address)
    URL = f"""https://github.com/makerdao/community/blob/master/governance/delegates/{checksummed_address}/profile.md"""
    page = requests.get(URL)
    
    if page.status_code == 404:

        URL = f"""https://github.com/makerdao/community/blob/master/governance/delegates/{address}/profile.md"""
        page = requests.get(URL)
        if page.status_code == 404:

            type = 'shadow'
            name = None
        
        else:
            
            type, name = _get_delegate_name(page)

    else:
        
        type, name = _get_delegate_name(page)

    return name, type
