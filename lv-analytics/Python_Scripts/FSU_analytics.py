
# coding: utf-8

# In[1]:

import datetime
import lv_query
import json
from itertools import chain
import string
import pandas as pd
import numpy as np
from timeit import default_timer as timer
import re


# In[2]:

# #employment status

# query = """
# select lvpe.key, lvpe.value, lvpe.user_id from lv_profile_entry lvpe
#     where lvpe.key in (
#     'is-employed-fulltime',
#     'employment-status') 
# """
# result_query = lv_query.libran(query)
# employment_table = result_query.sort('key')


# In[3]:

# employment_status_filter = employment_table[(employment_table.key=='employment-status') & (employment_table.value=='i_am_employed_full_time')
#                        |(employment_table.key=='employment-status') & (employment_table.value=='Employed full-time')]


# In[4]:

# employment_table[(employment_table.key=='is-employed-fulltime') & (employment_table.value== 'true')]


# In[5]:

import datetime
date = datetime.date(2014,4,10)
str(date)


# In[6]:

query = """
select lvpe.key, lvpe.value, lvpe.user_id
FROM lv_profile_entry lvpe
    JOIN user u
    ON u.id = lvpe.user_id
    WHERE lvpe.key in (
    'is-employed-fulltime',
    'employment-status') 
     and lvpe.value in (
     'true',
     'i_am_employed_full_time', 'i_am_employed_part_time','starting_a_new_job','Freelancing/Self-employed','Employed part-time', 'Employed full-time')
"""

result_query_last_login = lv_query.libran(query)
employed_table = result_query_last_login.sort('key')
#employed_table['value'].value_counts()
employed_table.head()


# In[7]:

x_map_client = employed_table.value.str

def FULL_PART_TIME_MAP_CLIENT(str):
    if re.search("full-time", str):
        return "full-time"
    elif re.search("full_time", str):
        return "full-time"
    elif re.search("true", str):
        return "full-time"
    elif re.search("starting_a_new_job",str):
        return "starting-a-new-job"
    elif re.search("self-employed", str):
        return "self-employed"
    elif re.search("Freelancing", str):
        return "freelance"
    elif re.search("part_time", str):
        return "part-time"
    else:
        return None

employed_table['employment_type'] = employed_table['value'].apply(lambda x_map_client: FULL_PART_TIME_MAP_CLIENT(x_map_client))


# In[8]:

# employed_table


# In[9]:

no_duplicates_employed = employed_table.drop_duplicates('user_id')
no_duplicates_employed = no_duplicates_employed.drop('key', axis=1)
# no_duplicates_employed


# In[10]:

no_duplicates_employed.columns = ['employment_status', 'user_id', 'employment_type']
no_duplicates_employed['employment_status'] = no_duplicates_employed['employment_status'].apply(lambda x: x is not None)
# no_duplicates_employed


# In[11]:

query = """
select lvpe.key, lvpe.value, lvpe.user_id, CURDATE()
FROM lv_profile_entry lvpe
    JOIN user u
    ON u.id = lvpe.user_id
    where lvpe.key in (
    'employment-status')
    and lvpe.value in (
     'i_am_employed_full_time', 'i_am_employed_part_time','starting_a_new_job','Freelancing/Self-employed','Employed part-time',
     'i_am_an_entrepreneur', 'Employed full-time' )
"""

result_query_last_login = lv_query.libran(query)
employed_table2 = result_query_last_login.sort('key')
employed_table2['value'].value_counts()
count_t2 = employed_table2.count()
count_t2


# In[12]:

#owns real estate

query = """
select lvpe.key, lvpe.value, lvpe.user_id
FROM lv_profile_entry lvpe
    JOIN user u
    ON u.id = lvpe.user_id
    where lvpe.key in (
    'owns-real-estate',
    'owned-real-estate') 
    and lvpe.value not in ('false', "")

"""
rent_or_own = lv_query.libran(query)
rent_or_own_table = rent_or_own.sort('user_id')


# In[13]:

no_duplicates_own = rent_or_own_table.drop_duplicates('user_id')
no_duplicates_own = no_duplicates_own.drop('key', axis=1)
no_duplicates_own.columns = ['owns_property', 'user_id']
no_duplicates_own['owns_property'] = no_duplicates_own['owns_property'].apply(lambda x: x is not None)
# no_duplicates_own


# In[14]:

#FIRST MERGE TEST, unemployed <=> own
employed_own = pd.merge(no_duplicates_employed, no_duplicates_own, on='user_id', how='outer')
# employed_own.set_index('user_id')


# In[15]:

# merge_rent_employed = pd.merge(rent_or_own_table, employed_table, on='user_id', how='outer')
#.pivot(index='date', columns='variable', values='value')


# In[16]:

# merge_rent_employed.set_index(['user_id'])
# test_merge.drop_duplicates('user_id').pivot(index='user_id', columns='key_x', values = 'value_x')
# merge_rent_employed['owns_real_estate'] = np.where(merge_rent_employed['key_x'] == 'owns-real-estate')
# merge_rent_employed['owns-real-estate'] = merge_rent_employed


# In[17]:

query = """
select lvpe.key, lvpe.value, lvpe.user_id
FROM lv_profile_entry lvpe
    JOIN user u
    ON u.id = lvpe.user_id
    where lvpe.key in ('partner-employment-status')
"""

#and lvpe.value not like '%unemployed%'

partner_info = lv_query.libran(query)
partner_info.groupby(['user_id'])


# In[18]:

partner_info['value'].value_counts()


# In[19]:

# x_map = partner_info.value.str
# x_map


# In[20]:

# x_map = partner_info.ix[0,:]['value']
# x_map = partner_info.value.to_string

x_map = partner_info.value.str
def FULL_PART_TIME_MAP(str):
    if re.search("full-time", str):
        return "full-time"
    elif re.search("self-employed", str):
        return "self-employed"
    elif re.search("freelancing", str):
        return "freelance"
    elif re.search("stay-at-home-parent", str):
        return "stay-at-home-parent"
    elif re.search("full-time-student", str):
        return "full-time-student"
    elif re.search("part-time", str):
        return "part-time"
    else:
        return None

partner_info['partner_employment_type'] = partner_info['value'].apply(lambda x_map: FULL_PART_TIME_MAP(x_map))
# partner_info


# In[21]:

x = partner_info.ix[0,:]['value']

import re
def FULL_PART_TIME(string):
    if len(re.findall("full-time|part-time", string)) > 0:
        return 1
    elif len(re.findall("self-employed | freelancing", string)) > 0:
        return 0
    else:
        return None
partner_info['partner_employment_status'] = partner_info['value'].apply(lambda x: FULL_PART_TIME(x))


# In[22]:

# partner_info
# no_duplicates_partner_employment


# In[23]:

no_duplicates_partner_employment = partner_info.drop_duplicates('user_id')
no_duplicates_partner_employment = no_duplicates_partner_employment.drop('key', axis=1)
no_duplicates_partner_employment = no_duplicates_partner_employment.drop('value', axis=1)
# no_duplicates_partner_employment


# In[24]:

# employed_own


# In[25]:

#merge partner_employment
employed_own_partnerEmployment = pd.merge(employed_own, no_duplicates_partner_employment, on='user_id', how='outer')
# employed_own_partnerEmployment


# In[26]:

query = """
select lvpe.key, lvpe.value, lvpe.user_id
FROM lv_profile_entry lvpe
    JOIN user u
    ON u.id = lvpe.user_id
    where lvpe.key in ('partner-income-salary-yearly')
"""

#and lvpe.value not like '%unemployed%'

partner_info_salary = lv_query.libran(query)
partner_info_salary.groupby(['user_id'])
partner_info_salary = partner_info_salary.drop('key',axis=1)
partner_info_salary.columns = ['partner_salary_yearly','user_id']
# partner_info_salary


# In[27]:

#Merge Partner Salary
employed_own_partnerEmployment_partnerSalary = pd.merge(employed_own_partnerEmployment, partner_info_salary, on='user_id', how='outer')
# employed_own_partnerEmployment_partnerSalary


# In[28]:

#LAST LOGIN IN INFO

query = """
select u.id as user_id, u.hashed_id, u.last_login_on, datediff(curdate(), u.last_login_on) as days_since_last_login,
case when datediff(curdate(), last_login_on) <= 7 then 1 else 0 end as login_last_7_days,
case when datediff(curdate(), last_login_on) <= 14 then 1 else 0 end as login_last_14_days
from user u 
join (select distinct user_id from lv_profile_entry) lvpe
on u.id = lvpe.user_id
where date(u.CREATED_ON) > '""" + str(date) + """'

"""

result_query_last_login = lv_query.libran(query)
last_login_table = result_query_last_login
# last_login_table
# last_login_table.columns = ['user_id','hashed_id', 'login_last_7_days','login_last_14_days']
# len(last_login_table)


# In[29]:

last_login_table.head()


# In[30]:

# last_login_table


# In[31]:

#drop duplicate user_ids
last_login_table = last_login_table.drop_duplicates('user_id')


# In[32]:

employed_own_partnerEmployment_partnerSalary_lastLogin = pd.merge(last_login_table,employed_own_partnerEmployment_partnerSalary, on='user_id', how='outer')
final_merge = employed_own_partnerEmployment_partnerSalary_lastLogin.set_index('user_id')
# final_merge.loc[final_merge.owns_property == np.nan] = 0
# final_merge.ix[final_merge.owns_property == np.nan] = 0


# In[33]:

# final_merge = final_merge.replace(np.nan, 0)


# In[44]:

final_merge = final_merge.replace(True, 1)
final_merge.info()


# In[45]:

data = pd.read_csv('demographic_insights_2014_csv.csv',error_bad_lines=False, index_col=0)
data.columns


# In[46]:

final_merge_data = pd.merge(data, final_merge, on='hashed_id', how ='left')


# In[47]:

final_merge_data.info()


# In[48]:

# final_merge_data = final_merge_data.replace(np.nan, 0)
final_merge_data.columns
# final_merge_data.info()


# In[49]:

#final_merge_data.drop('Unnamed: 0', 1, inplace=True)
final_merge_data = final_merge_data.rename(columns={'Unnamed: 0': 'unnamed'})


# In[50]:

import pymysql


con = pymysql.connect("kiwi.learnvest.com",
                          "root",
                          "learnve5t",
                          "datawarehouse",
                          3306)


# In[51]:

final_merge_data.head()


# In[53]:

final_merge_data.info()


# In[52]:

final_merge_data.to_sql('FSU_client_data', con, flavor="mysql", if_exists="append", index=False)


# In[ ]:

# export_merge_csv = final_merge.to_csv('FSU_analytics_merged.csv')


# In[ ]:

# values_final_merge = final_merge.values
# values_final_merge
# def normalize_fields():
#     if values_final_merge == 'true':
#         return 1
#     elif values_final_merge == 'nan':
#         return 0
#     elif values_final_merge == None:
#         return 0
#     elif values_final_merge== True:
#         return 1
#     else:
#         None
# values_final_merge = np.vectorize(lambda x: normalize_fields())

