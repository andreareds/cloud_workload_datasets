#!/usr/bin/env python
# coding: utf-8

# In[33]:

#Preprocessing

import numpy as np
import pandas as pd
import glob


jobfiles = []

PATH = "/projects/arossi/gctv2/clusterdata-2011-2/"

usagefiles = sorted(glob.glob(PATH + "task_usage/extracted/*.csv"))


usage_header_list = ['start time', 'end time', 'job ID', 'task index', 'machine ID', 'CPU rate',
                     'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum CPU rate', 'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage']
"""
SQL equivalent for cluster 2019
SELECT CAST(start_time/300000000 as int) as slot, SUM(average_usage.cpus) as avgcpu, SUM(average_usage.memory) as avgmem, SUM(maximum_usage.cpus) as maxcpu, SUM(maximum_usage.memory) as maxmem
FROM `google.com:google-cluster-data`.clusterdata_2019_h.instance_usage
WHERE collection_type = 0
GROUP BY slot
ORDER BY slot
"""

df_usage = pd.DataFrame()
print(len(df_usage))
for i, f in enumerate(usagefiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=usage_header_list)
    tmp = tmp.drop(columns=['machine ID', 'unmapped page cache',
                     'total page cache','disk I/O time','local disk space usage',
                     'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage'])
    df_usage = pd.concat([df_usage, tmp])

print("DATASET COLUMNS", df_usage.columns)

df_usage['slot'] = (df_usage['start time']/300000000).astype(int)
grouped = df_usage.groupby(by=["slot"]).sum()
grouped = grouped.sort_values(by=['slot'])
grouped.to_csv(PATH+"resources.csv", index=False)
