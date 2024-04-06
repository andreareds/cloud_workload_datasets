#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd
import glob
"""
PATH = "batch_instance.csv"

COLUMNS = ["instance_name", "task_name", "job_name", "task_type", "status", "start_time", "end_time", "machine_id",
           "seq_no", "total_seq_no","cpu_avg","cpu_max","mem_avg","mem_max"]

WIN = 5*60 #5 minutes window in seconds
a = pd.read_csv(PATH, names=COLUMNS, chunksize=100000)
final = pd.DataFrame(None, columns= COLUMNS)
for i,df in enumerate(a):
    print(i)
    df['time_inter'] = df['end_time'].values - df['start_time'].values
    df['slot'] = df['start_time'].values//WIN
    df.to_csv("time_inter_"+str(i)+".csv")
    df_group = df.groupby(by=['slot']).sum()
    df_group.to_csv("slotted_2018_"+str(i)+".csv")
    #df_group['slot'] = df_group.index
    #final = pd.concat([final, df_group])
#final = final.groupby(by=['slot']).sum()
#final.to_csv("slotted_2018.csv")
"""
files = sorted(glob.glob("alibis/group/*"))
columns = pd.read_csv(files[0]).columns
merged_df = pd.DataFrame(None, columns=columns)
for i, f in enumerate(files):
    df = pd.read_csv(f)
    merged_df = pd.concat([merged_df, df])
    #df_group = df.groupby(by=["slot"]).sum()
merged_df.to_csv("alibis/group/merged.csv")
