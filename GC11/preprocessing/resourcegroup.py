#!/usr/bin/env python
# coding: utf-8

# In[33]:

#Preprocessing

import numpy as np
import pandas as pd
import glob


PATH = "/projects/arossi/gctv2/clusterdata-2011-2/"
"""
usagefiles = sorted(glob.glob(PATH + "task_usage/extracted/*.csv"))


usage_header_list = ['start time', 'end time', 'job ID', 'task index', 'machine ID', 'CPU rate',
                     'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum CPU rate', 'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage']

SQL equivalent for cluster 2019
SELECT CAST(start_time/300000000 as int) as slot, SUM(average_usage.cpus) as avgcpu, SUM(average_usage.memory) as avgmem, SUM(maximum_usage.cpus) as maxcpu, SUM(maximum_usage.memory) as maxmem
FROM `google.com:google-cluster-data`.clusterdata_2019_h.instance_usage
WHERE collection_type = 0
GROUP BY slot
ORDER BY slot


df_usage = pd.DataFrame()
for i, f in enumerate(usagefiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=usage_header_list)
    tmp = tmp.drop(columns=['machine ID', 'unmapped page cache',
                     'total page cache','disk I/O time','local disk space usage',
                     'maximum disk IO time', 'cycles per instruction',
"""
a = pd.read_csv(PATH+"df_usage.csv", chunksize=100000)
for i,df_usage in enumerate(a):
    print(i)
    df_usage['interval'] = df_usage['end time'].values - df_usage['start time'].values
    df_usage['avgcpu'] = df_usage['CPU rate'].values * df_usage['interval'].values
    df_usage['avgmem'] = df_usage['canonical memory usage'].values * df_usage['interval'].values
    grouped = df_usage.groupby(by=["slot"]).sum()
    #grouped = grouped.sort_values(by=['slot'])
    grouped.to_csv(PATH+"resources_"+str(i)+".csv")#, index=False)
