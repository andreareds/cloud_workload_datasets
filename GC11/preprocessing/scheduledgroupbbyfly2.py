#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob

columns = ['time', 'task index', 'priority', 'CPU request', 'memory request',
       'disk space request', 'start time', 'end time', 'CPU rate',
       'canonical memory usage', 'assigned memory usage',
       'maximum memory usage', 'maximum CPU rate', 'interval', 'cpusecs',
       'memsecs']

PATH = "/projects/arossi/gctv2/clusterdata-2011-2/"


merged_dfmin = pd.read_csv(PATH+"scheduledcompleteMin1.csv", names=columns)
merged_dfmin = merged_dfmin.sort_values(by=['time'])
merged_dfmin['job ID'] = merged_dfmin.index
merged_dfmin = merged_dfmin.groupby(['job ID']).min()

merged_dfsum = pd.read_csv(PATH+"scheduledcompleteSum1.csv", names=columns)
merged_dfsum = merged_dfsum.sort_values(by=['time'])
merged_dfsum['job ID'] = merged_dfsum.index
merged_dfsum = merged_dfsum.groupby(['job ID']).sum()

final_df = pd.DataFrame(columns=["job ID", 'time', 'CPU request', 'memory request',
       'disk space request', 'CPU rate',
       'canonical memory usage', 'assigned memory usage',
       'maximum memory usage', 'maximum CPU rate', 'interval', 'cpusecs',
       'memsecs'])
final_df['time'] = merged_dfmin['time'].values
final_df['job ID'] = merged_dfmin.index
final_df['interval'] = merged_dfsum['interval'].values
final_df['cpusecs'] = merged_dfsum['cpusecs'].values
final_df['memsecs'] = merged_dfsum['memsecs'].values
final_df['CPU request'] = merged_dfsum['CPU request'].values
final_df['CPU rate'] = merged_dfsum['CPU rate'].values
final_df['memory request'] = merged_dfsum['memory request'].values
final_df['disk space request'] = merged_dfsum['disk space request'].values
final_df['canonical memory usage'] = merged_dfsum['canonical memory usage'].values
final_df['assigned memory usage'] = merged_dfsum['assigned memory usage'].values
final_df['maximum memory usage'] = merged_dfsum['maximum memory usage'].values
final_df['maximum CPU rate'] = merged_dfsum['maximum CPU rate'].values
final_df = final_df.sort_values(by=['time'])
final_df.to_csv("scheduledDataset.csv")

