
# coding: utf-8

# In[3]:

import datetime
import lv_query
import json
from itertools import chain
import string
import pandas as pd
import numpy as np
from timeit import default_timer as timer


# In[4]:

abandoned_users = pd.read_csv('video_abandoned.csv')
completed_users = pd.read_csv('video_complete.csv')


# In[5]:

abandoned_users = abandoned_users['UserID']
completed_users = completed_users['UserID']


# In[6]:

abandoned = pd.DataFrame(pd.Series(abandoned_users))
abandoned['video_status'] = 'abandoned'
completed = pd.DataFrame(pd.Series(completed_users))
completed['video_status'] = 'completed'


# In[7]:

horizontal_stack = pd.concat([completed, abandoned], axis=0)


# In[8]:

horizontal_stack


# In[9]:

#these users have completed their profile

query = """select u.hashed_id
from onboarding_profile_status obps
join user u
on u.id = obps.user_id
where last_saved_point IN ("#/snapshot", "#/submission");
"""
result_query = lv_query.libran(query)
profile_complete = result_query
profile_complete['profile_status'] = 'completed'
profile_complete.columns = ['UserID', 'profile_status']


# In[10]:

profile_complete


# In[11]:

merged_data = pd.merge(horizontal_stack,profile_complete, how='left', on=['UserID'])


# In[12]:

merged_data
merged_data['video_completed'] = merged_data['video_status'].apply(lambda x: x == "completed")
merged_data['profile_completed'] = merged_data['profile_status'].apply(lambda x: x == 'completed')
merged_data.head()


# In[13]:

merged_data['profile_completed'].sum()


# In[14]:

# percentage of profile completion for those who have completed video/have not
groupedbyvideo = merged_data.groupby('video_completed').mean()
groupedbyprofile = merged_data.groupby('profile_completed').mean()


# In[15]:

groupedbyvideo


# In[16]:

groupedbyprofile


# In[17]:

#correlation
from scipy.stats.stats import pearsonr
x = merged_data['video_completed'].values
y = merged_data['profile_completed'].values
print "correlation : " + str(pearsonr(x,y)[0]*100) + '%'  


# In[23]:




# In[ ]:



