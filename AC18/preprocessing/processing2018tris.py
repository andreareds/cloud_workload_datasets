#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd
import glob

PATH = "batch_instance.csv"
"""
COLUMNS = ["instance_name", "task_name", "job_name", "task_type", "status", "start_time", "end_time", "machine_id",
           "seq_no", "total_seq_no","cpu_avg","cpu_max","mem_avg","mem_max"]

WIN = 5*60 #5 minutes window in seconds
a = pd.read_csv(PATH, names=COLUMNS, chunksize=100000)
final = pd.DataFrame(None, columns= COLUMNS)
for i,df in enumerate(a):
    print(i)
    df['time_inter'] = df['end_time'].values - df['start_time'].values
    df['slot'] = df['start_time'].values//WIN
    df['avgcpu'] = df['cpu_avg'].values*df['time_inter'].values
    df['avgmem'] = df['mem_avg'].values*df['time_inter'].values
    df['maxcpu'] = df['cpu_max'].values*df['time_inter'].values
    df['maxmem'] = df['mem_max'].values*df['time_inter'].values
    
    df_group = df.groupby(by=['slot']).sum()
    df_group.to_csv("Ali2018_"+str(i)+".csv")

"""
files = sorted(glob.glob("Ali2018_*.csv"))
columns = pd.read_csv(files[0]).columns
df_ali = pd.DataFrame(None, columns=columns)
for f in files:
    df = pd.read_csv(f)
    df_ali = pd.concat([df_ali, df])
df_ali.to_csv("Alibaba2018.csv")

#df_group['slot'] = df_group.index
    #final = pd.concat([final, df_group])
#final = final.groupby(by=['slot']).sum()
#final.to_csv("slotted_2018.csv")

