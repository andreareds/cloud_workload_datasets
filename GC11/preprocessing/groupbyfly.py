#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob
import time
from alive_progress import alive_bar

PATH = "/projects/arossi/gctv2/clusterdata-2011-2/join/"

df_task = pd.read_csv("taskTotal.csv")
df_task = df_task.drop_duplicates(subset=["job ID", "task index"])
df_task = df_task.loc[df_task.time >= 600000000]


def preprocess(x):
    merged_df=pd.merge(df_task,x, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])
    merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
    merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
    merged_dfsum = merged_df.groupby(['job ID']).sum()
    #merged_dfsum['job ID'] = merged_dfsum.index.values
    #merged_dfsum = merged_dfsum.reset_index()
    merged_dfsum.to_csv(PATH+"sum.csv",mode="a",header=False)
    merged_dfmin = merged_df.groupby(['job ID']).min()
    #merged_dfmin['job ID'] = merged_dfmin.index.values
    #merged_dfmin = merged_dfmin.reset_index()
    merged_dfmin.to_csv(PATH+"min.csv",mode="a",header=False)
    #Insert new merge

    # In[ ]:
    """

    final_df = pd.DataFrame(columns=['time', 'job ID', 'interval', 'cpusecs'])
    final_df['time'] = merged_dfmin['time'].values
    final_df['job ID'] = merged_dfmin.index.values
    final_df['interval'] = merged_dfsum['interval'].values
    final_df['cpusecs'] = merged_dfsum['cpusecs'].values
    final_df = final_df.sort_values(by=['time'])
    final_df.to_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/dataset.csv",mode="a",header=False,index=False)
    """

reader = pd.read_csv("usageTotal.csv", chunksize=30000) # chunksize depends with you colsize

#reader = pd.read_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/df4.csv", chunksize=30000, names=["time", "job ID", "task", "start time", "end time", "CPU rate"])

TOTAL_CHUNKS = 41092
TOTAL_LINES = 1232739308

for i,r in enumerate(reader):
    with alive_bar(i) as bar:
        print("PROCESSED CHUNKS:" + str(i) + "out of " + str(TOTAL_CHUNKS))
        preprocess(r)
        bar()
    
merged_df = pd.read_csv(PATH+"sum.csv", names=['time', 'task index', 'start time', 'end time', 'CPU rate', 'interval', 'cpusecs',
                                              'job ID'])
merged_dfsum = merged_df.groupby(['job ID']).sum()
merged_dfsum = merged_dfsum.filter(items=['time', 'cpusecs', 'interval', 'job ID'])




merged_df = pd.read_csv(PATH+"min.csv", names=['time', 'task index', 'start time', 'end time', 'CPU rate', 'interval', 'cpusecs',
                                              'job ID'])
merged_dfmin = merged_df.groupby(['job ID']).min()
merged_dfmin = merged_dfmin.filter(items=['time', 'cpusecs', 'interval', 'job ID'])




final_df = pd.DataFrame(columns=['time', 'job ID', 'interval', 'cpusecs'])
final_df['time'] = merged_dfmin['time'].values
final_df['job ID'] = merged_dfmin.index.values
final_df['interval'] = merged_dfsum['interval'].values
final_df['cpusecs'] = merged_dfsum['cpusecs'].values
final_df = final_df.sort_values(by=['time'])
final_df.to_csv(PATH + "dataset.csv")