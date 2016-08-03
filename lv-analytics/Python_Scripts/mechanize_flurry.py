
# coding: utf-8

# In[4]:

import mechanize
import cookielib
import csv
import requests
import urllib2
import urllib
from cookielib import CookieJar
import logging
import sys
# sys.path.append('/imports/')
# import cred
import json
import os
import logging.handlers
import pymysql
import sqlalchemy
import psycopg2
import cgi
import re
import pandas as pd
from pandas import read_sql


# In[2]:

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')


# In[15]:

# Redshift Connection
engine = sqlalchemy.create_engine("postgresql+psycopg2://bw_scripts:ETPoP4x0a8L2@dw-poc.ct570ptiatav.us-east-1.redshift.amazonaws.com:5439/dw_prod")
logging.info("successfully connected to Redshift")
result = engine.execute("SELECT max(timestamp) FROM nmios_user_log;")
max_timestamp = result.fetchone()
value = max_timestamp.values()
from datetime import datetime
str_value = str(value).strip('[]')
byte_str = bytearray(str_value)
del byte_str[0]
str_value = str(byte_str)
str_value = str_value.replace("'",'')
str_value
from dateutil.parser import parse
datetime_str = parse(str_value)


# In[7]:

# import timedelta
from datetime import datetime, timedelta
import datetime
date_now = datetime.datetime.now().date()
d = date_now - timedelta(days=1)
endDate = str(date_now).replace('-','_')
startDate = str(d).replace('-','_')
lastDateObj = datetime_str.date()
lastDate = str(lastDateObj).replace('-','_')

# "".join(y)

#custom cut

# time_cut_url = """https://dev.flurry.com/analyticsBehaviorEventsLogsAll.do?projectID=854789&versionCut=versionsAll&intervalCut=customInterval""" + "".join(startDate) + """-""" + "".join(endDate) + """"""
time_cut_url = """https://dev.flurry.com/eventsLogCsv.do?projectID=854789&versionCut=versionsAll&intervalCut=customInterval""" + "".join(lastDate) + """-""" + "".join(endDate) + """&childProjectId=0&stream=true&direction=1&offset=0"""

lastruntime = 'flurry_LastRunTime.txt'


# In[8]:

time_cut_url


# In[9]:

def ReadLastRunTime():
    with open(lastruntime, 'r') as f:
        data = f.read()
        date = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
        return date

def LogTimestamp():
    date_format = '%Y-%m-%d %H:%M:%S.%f'
    test_date = datetime.datetime.now().strftime(date_format)

    with open(lastruntime,'w') as outf:
        outf.write(test_date)


# In[10]:

# Browser instance
br = mechanize.Browser()
logging.info("creating browser instance with mechanize")
# Cookie Jar
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

# Browser options
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)

# Follows refresh 0 but not hangs on refresh > 0
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

#debug
br.set_debug_http(True)
br.set_debug_redirects(True)
br.set_debug_responses(True)


br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
auth_url = 'https://dev.flurry.com/secure/loginAction.do'
br.open(auth_url)
logging.info("opening up auth URL")

#print form
# for f in br.forms():
# 	print f

br.select_form(nr=0)
#add email and password to form
br.form['loginEmail'] = 'wla@learnvest.com'
br.form['loginPassword'] = 'resetagain!'
#submit
br.submit()
logging.info("submitted auth form")


# In[11]:

csv_url = 'https://dev.flurry.com/eventsLogCsv.do?projectID=854789&versionCut=versionsAll&intervalCut=30Days&childProjectId=0&stream=true&direction=1&offset=0'
events_csv = br.open(time_cut_url)
logging.info("opening up events_csv URL to download")
content = csv.reader(events_csv)
logging.info("save content with csv.reader")

logging.info("grabbing filename from content-disposition")
info = events_csv.info()
content_header = info.getheader('Content-Disposition')
value, params = cgi.parse_header(content_header)
file_name = params['filename']

logging.info("write the content to a csv file")
with open(file_name, 'wb') as f:
    writer = csv.writer(f)
    writer.writerows(content)
    f.close()


# In[212]:

# print info


# In[45]:

logging.info("write to df")
df = pd.read_csv(file_name)
logging.info("properly mapped to a df")


# In[46]:

df = df.rename(columns={'Unnamed: 9': 'unnamed'})
df = df.rename(columns={'Session Index':'session_index'})
df = df.rename(columns={'User ID':'user_id'})
df['Params'] = df['Params'].map(lambda x: x.lstrip('{').rstrip('}'))
# df['user_id'] = df['user_id'].apply(lambda x: x == 'Unknown User Id')
# remap_user_id()


# In[42]:

for idx, series in df.iterrows():
    if series['user_id'] == 'Unknown User Id':
        df['user_id'] = df['Params']
    else:
        None


# In[50]:

df


# In[49]:

# # for user in df['user_id']:
# #         for param in df['Params']:
# from itertools import izip
# for x, y in izip(df['user_id'], df['Params']):
#     if x == 'Unknown User Id':
#         df['user_id'] = df['Params']
#     elif x != 'Unknown User Id':
#         None           


# In[18]:

# for x in df['user_id']:
#     print "x" if df['user_id'] == 'Unknown User Id'


# In[44]:

def WriteToRedshift():
    df.to_sql('nmios_user_events_log', engine, if_exists="append", index=False)
    return 'SUCCESS'
    logging.info("written to redshift")

def LogToFile():
    if WriteToRedshift() == 'SUCCESS':
        LogTimestamp()
        logging.info("write to log file for flurry analytics csv")
    else:
        logging.info('script has failed')

LogToFile()


# In[199]:

# result = engine.execute("SELECT user_id, params, event, timestamp FROM nmios_user_events_log WHERE event = 'View : Summary Screen' OR event = 'Login : Login Button'")
# user_info_query = result.fetchall()


# In[201]:

# user_info_df = pd.DataFrame(user_info_query)


# In[210]:

# user_info_df.columns = ['user_id', 'params', 'event', 'timestamp']


# In[234]:

# user_info_df.head()


# In[ ]:

#view summary screen event and replace user_id with params where unknown

