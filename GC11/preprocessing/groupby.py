#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob




def preprocess(merged_df):
    print(merged_df)

    merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
    merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
    merged_dfsum = merged_df.groupby(['job ID']).sum()
    merged_dfsum.to_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/sum.csv",mode="a",header=False,index=False)
    merged_dfmin = merged_df.groupby(['job ID']).min()
    merged_dfmin.to_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/min.csv",mode="a",header=False,index=False)


    # In[ ]:


    final_df = pd.DataFrame(columns=['time', 'job ID', 'interval', 'cpusecs'])
    final_df['time'] = merged_dfmin['time'].values
    final_df['job ID'] = merged_dfmin.index.values
    final_df['interval'] = merged_dfsum['interval'].values
    final_df['cpusecs'] = merged_dfsum['cpusecs'].values
    final_df = final_df.sort_values(by=['time'])
    final_df.to_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/dataset.csv",mode="a",header=False,index=False)



reader = pd.read_csv("/projects/arossi/gctv2/clusterdata-2011-2/join/df4.csv", chunksize=30000, names=["time", "job ID", "task", "start time", "end time", "CPU rate"])


[preprocess(r) for r in reader]
