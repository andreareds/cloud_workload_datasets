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
taskfiles = sorted(glob.glob(PATH + "task_events/extracted/*.csv"))

job_header_list = ['time', 'missing info', 'job ID', 'event type', 'user', 'scheduling class','job name','logical job name']

usage_header_list = ['start time', 'end time', 'job ID', 'task index', 'machine ID', 'CPU rate',
                     'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum CPU rate', 'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage']

task_header_list = ['time', 'missing info', 'job ID', 'task index', 'machine ID',
               'event type','user','scheduling class','priority','CPU request',
               'memory request','disk space request','different machines restriction']

"""
df_usage = pd.DataFrame()
for i, f in enumerate(usagefiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=usage_header_list)
    tmp = tmp.drop(columns=['machine ID', 'canonical memory usage', 'assigned memory usage', 'unmapped page cache',
                     'total page cache', 'maximum memory usage','disk I/O time','local disk space usage',
                     'maximum disk IO time', 'cycles per instruction',
                     'memory accesses per instruction','sample portion','aggregation type', 'sampled CPU usage'])
    df_usage = pd.concat([df_usage, tmp])
df_usage.to_csv(PATH + "completeUsageTotal.csv", index=False)
del(df_usage)
"""

df_task = pd.DataFrame()
for i, f in enumerate(taskfiles):
    if i%100==0:
        print("file", f, "in processing")
    tmp = pd.read_csv(f, names=task_header_list)
    tmp = tmp.loc[tmp['event type'] == 0]
    tmp = tmp.drop(columns=['missing info', 'machine ID',
               'event type','user','scheduling class','different machines restriction'])
    tmp = tmp.loc[(tmp.time >= 600000000) & tmp.time < 2e63-1]
    df_task = pd.concat([df_task, tmp])
df_task.to_csv(PATH + "completeTaskTotal.csv", index=False)
