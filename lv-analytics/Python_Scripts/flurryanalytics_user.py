import json
import collections
import urllib
import requests
import pandas as pd
from datetime import datetime
from datetime import date, timedelta

date_now = datetime.now().date()
d = date_now - timedelta(days=1)
endDate = str(date_now)
startDate = str(d)
# "".join(y)
url = """http://api.flurry.com/eventMetrics/Event?apiAccessCode=PMGKJ5GNTYPZNBRXZCQT&apiKey=57Q7DKCNWMXSQDRRNMPG&startDate=""" + "".join(startDate) + """&endDate=""" + "".join(endDate) + """&eventName=Login%20%3A%20Login%20Button"""

data = json.load(urllib.urlopen(url))
pretty_json = json.dumps(data, indent=4, sort_keys=True)
# print pretty_json
user_info = data['parameters']['key']['value']
user_df = pd.DataFrame(user_info)
user_df