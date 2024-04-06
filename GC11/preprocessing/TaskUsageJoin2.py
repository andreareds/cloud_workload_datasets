#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob



df_task = pd.read_csv("taskTotal.csv")
df_task


# In[ ]:


def preprocess(x):
    df2=pd.merge(df_task,x, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])
    df2.to_csv("df4.csv",mode="a",header=False,index=False)

reader = pd.read_csv("usageTotal.csv", chunksize=30000) # chunksize depends with you colsize

[preprocess(r) for r in reader]


# In[ ]:


merged_df = pd.read_csv("df4.csv", chunksize=30000)


# In[ ]:


merged_df['interval'] = (merged_df['end time'].values - merged_df['start time'].values)/1e6
merged_df['cpusecs'] = merged_df['interval'] * merged_df['CPU rate']
merged_dfsum = merged_df.groupby(['job ID']).sum()
merged_dfsum.to_csv("sum.csv", index=False)
merged_dfmin = merged_df.groupby(['job ID']).min()
merged_dfmin.to_csv("min.csv", index=False)


# In[ ]:


final_df = pd.DataFrame(columns=['time', 'job ID', 'interval', 'cpusecs'])
final_df['time'] = merged_dfmin['time'].values
final_df['job ID'] = merged_dfmin.index.values
final_df['interval'] = merged_dfsum['interval'].values
final_df['cpusecs'] = merged_dfsum['cpusecs'].values
final_df = final_df.sort_values(by=['time'])
final_df.to_csv("dataset.csv")
