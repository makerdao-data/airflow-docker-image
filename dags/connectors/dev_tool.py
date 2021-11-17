#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import json


def get_executives_url():

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    caps = webdriver.DesiredCapabilities.CHROME.copy()
    caps['acceptInsecureCerts'] = True
    caps["goog:loggingPrefs"] = {"performance": "ALL"}  # chromedriver 75+

    driver = webdriver.Remote(
        options=options, desired_capabilities=caps, command_executor="http://localhost:4444/wd/hub"
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
