#!/usr/bin/env python
# coding: utf-8

# In[33]:


import numpy as np
import pandas as pd
import glob


# In[77]:


jobfiles = []


# In[78]:


usagefiles = sorted(glob.glob("/projects/arossi/gctv2/clusterdata-2011-2/task_usage/extracted/*.csv"))


# In[79]:


taskfiles = sorted(glob.glob("/projects/arossi/gctv2/clusterdata-2011-2/task_events/extracted/*.csv"))


# In[80]:


job_header_list = ['time', 'missing info', 'job ID', 'event type', 'user', 'scheduling class','job name','logical job name']


# In[81]:


usage_header_list = ['start time', 'end time', 'job ID', 'task index', 'machine ID', 'CPU rate',
                     'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum CPU rate', 'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage']


# In[82]:


task_header_list = ['time', 'missing info', 'job ID', 'task index', 'machine ID',
               'event type','user','scheduling class','priority','CPU request',
               'memory request','disk space request','different machines restriction']


# In[48]:


df_usage = pd.DataFrame()
for i, f in enumerate(usagefiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=usage_header_list)
    tmp = tmp.drop(columns=['machine ID', 'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum CPU rate', 'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage'])
    df_usage = pd.concat([df_usage, tmp])


# In[51]:


df_task = pd.DataFrame()
for i, f in enumerate(taskfiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=task_header_list)
    tmp = tmp.loc[tmp['event type'] == 0]
    tmp = tmp.drop(columns=['missing info', 'machine ID',
               'event type','user','scheduling class','priority','CPU request',
               'memory request','disk space request','different machines restriction'])
    tmp = tmp.loc[(tmp.time >= 600000000) & tmp.time < 2e63-1]
    df_task = pd.concat([df_task, tmp])


# In[52]:


merged_df = pd.merge(df_usage, df_task, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])

print(merged_df)


# In[70]:


merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
merged_dfsum = merged_df.groupby(['job ID']).sum()
merged_dfsum
print(merge_dfsum)


# In[71]:


merged_dfmin = merged_df.groupby(['job ID']).min()
merged_dfmin
print(merged_dfmin)


# In[76]:


final_df = pd.DataFrame(columns=['time', 'job ID', 'interval', 'cpusecs'])
final_df['time'] = merged_dfmin['time'].values
final_df['job ID'] = merged_dfmin.index.values
final_df['interval'] = merged_dfsum['interval'].values
final_df['cpusecs'] = merged_dfsum['cpusecs'].values
final_df = final_df.sort_values(by=['time'])
#final_df['date'] = pd.to_datetime(final_df['time'], unit='us')
final_df.to_csv("dataset.csv")
print(final_df)

