#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd

PATH = "batch_instance.csv"

COLUMNS = ["instance_name", "task_name", "job_name", "task_type", "status", "start_time", "end_time", "machine_id",
           "seq_no", "total_seq_no","cpu_avg","cpu_max","mem_avg","mem_max"]

WIN = 5*60 #5 minutes window in seconds
a = pd.read_csv(PATH, names=COLUMNS, chunksize=1000)
final = pd.DataFrame(None, columns= COLUMNS)
for df in a:
    df['time_inter'] = df['end_time'].values - df['start_time'].values
    df['slot'] = df['start_time'].values//WIN
    df_group = df.groupby(by=['slot']).sum()
    df_group['slot'] = df_group.index
    final = pd.concat([final, df_group])
final = final.groupby(by=['slot']).sum()
final.to_csv("slotted_2018.csv")

