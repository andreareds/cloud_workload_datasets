#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob
import time
from alive_progress import alive_bar

columns = []
def preprocess(i, x, p):
    merged_df=pd.merge(df_task,x, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])
    merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
    merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
    if i==0:
        columns = merged_df.columns
        print(columns)
    merged_dfsum = merged_df.groupby(['job ID']).sum()
    merged_dfsum.to_csv(PATH+"completeSumPr"+str(p)+".csv",mode="a",header=False)
    merged_dfmin = merged_df.groupby(['job ID']).min()
    merged_dfmin.to_csv(PATH+"completeMinPr"+str(p)+".csv",mode="a",header=False)

PATH = "/projects/arossi/gctv2/clusterdata-2011-2/"

MAX_PRIORITY = 11
for p in range(8, MAX_PRIORITY):
    df_task = pd.read_csv(PATH+"completeTaskTotal.csv")
    df_task = df_task.drop_duplicates(subset=["job ID", "task index"])
    df_task = df_task.loc[df_task.time >= 600000000]
    df_task = df_task.loc[df_task.priority == p]


    reader = pd.read_csv(PATH+"completeUsageTotal.csv", chunksize=10000) # chunksize depends with you colsize

#reader = pd.read_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/df4.csv", chunksize=30000, names=["time", "job ID", "task", "start time", "end time", "CPU rate"])

    TOTAL_CHUNKS = 41092
    TOTAL_LINES = 1232739308
    
    for i, r in enumerate(reader):
        print("PROCESSED CHUNKS:" + str(i) + " with priority " + str(p))
        preprocess(i, r, p)
 
    merged_df = pd.read_csv(PATH+"completeSumPr"+str(p)+".csv", names=columns)
    merged_df['job ID'] = merged_df.index
    merged_dfsum = merged_df.groupby(['job ID']).sum()
    merged_dfsum.to_csv(PATH+"completeSumPr"+str(p)+".csv")



    merged_df = pd.read_csv(PATH+"completeMinPr"+str(p)+".csv", names=columns)
    merged_df['job ID'] = merged_df.index
    merged_dfmin = merged_df.groupby(['job ID']).min()
    merged_dfmin.to_csv(PATH+"completeMinPr"+str(p)+".csv")

