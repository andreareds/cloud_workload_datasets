#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob

columns = []
def preprocess(i, x):
    merged_df=pd.merge(df_task,x, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])
    merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
    merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
    merged_df['memsecs'] = merged_df['interval'] * merged_df['assigned memory usage']
    if i==0:
        columns = merged_df.columns
        print(columns)
    merged_dfsum = merged_df.groupby(['job ID']).sum()
    merged_dfsum.to_csv(PATH+"scheduledcompleteSum1.csv",mode="a",header=False)
    merged_dfmin = merged_df.groupby(['job ID']).min()
    merged_dfmin.to_csv(PATH+"scheduledcompleteMin1.csv",mode="a",header=False)

PATH = "/projects/arossi/gctv2/clusterdata-2011-2/"


df_task = pd.read_csv(PATH+"scheduledTaskTotal.csv")
df_task = df_task.drop_duplicates(subset=["job ID", "task index"])
df_task = df_task.loc[df_task.time >= 600000000]

reader = pd.read_csv(PATH+"memoryUsageTotal.csv", chunksize=10000) # chunksize depends with you colsize

for i, r in enumerate(reader):
    print("PROCESSED CHUNKS:" + str(i))
    preprocess(i, r)


merged_df = pd.read_csv(PATH+"scheduledcompleteSum1.csv", names=columns)
merged_dfsum = merged_df.groupby(['job ID']).sum()
merged_df.to_csv(PATH+"scheduledcompleteSum1.csv")




merged_df = pd.read_csv(PATH+"scheduledcompleteMin1.csv", names=columns)
merged_dfmin = merged_df.groupby(['job ID']).min()
merged_dfmin.to_csv(PATH+"scheduledcompleteMin1.csv")
