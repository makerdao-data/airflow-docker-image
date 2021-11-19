from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json


def _get_executives_url():

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    caps = webdriver.DesiredCapabilities.CHROME.copy()
    caps['acceptInsecureCerts'] = True
    caps["goog:loggingPrefs"] = {"performance": "ALL"}  # chromedriver 75+

    driver = webdriver.Remote(
        options=options,
        command_executor="new-snowflake-airflow-docker-image_selenium_1:4444/wd/hub",
        desired_capabilities=caps,
    )

    driver.get("https://vote.makerdao.com/executive?network=mainnet")
    logs = driver.get_log("performance")

    driver.close()
    driver.quit()

    for i in logs:

        temp = json.loads(i['message'])
        if temp['message']['method'] == 'Network.requestWillBeSent':
            if 'executive.json' in temp['message']['params']['request']['url']:
                url = temp['message']['params']['request']['url']

    return url
