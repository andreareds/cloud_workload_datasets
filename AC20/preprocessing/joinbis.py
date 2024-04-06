#!/usr/bin/env python
# coding: utf-8

# In[43]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob

JOIN_PATH = "instance_sensor_join_table.csv"
WIN = 5*60 #5 minutes window

join_columns = ['Unnamed: 0', 'job_name', 'task_name', 'inst_name', 'worker_name',
                 'inst_id', 'status', 'start_time', 'end_time', 'machine', 'gpu_name',
                 'cpu_usage', 'gpu_wrk_util', 'avg_mem', 'max_mem', 'avg_gpu_wrk_mem',
                 'max_gpu_wrk_mem', 'read', 'write', 'read_count', 'write_count', 'slot',
                 'len_inter', 'num_slots']



join_file = pd.read_csv(JOIN_PATH)#, names=join_columns)

join_file['len_inter'] = join_file['end_time'].values - join_file['start_time'].values
join_file['num_slots'] = (join_file['len_inter'].values // WIN) + 1

total_slots = (join_file['end_time'].max() - join_file['start_time'].min())// WIN + 1
START_TIME = join_file['start_time'].min()
END_TIME = join_file['end_time'].max()
"""
join_copy = pd.DataFrame(None, columns=join_file.columns)
print("Total rows:", join_file.shape[0])
for i_r, row in enumerate(join_file.values):
    if i_r%100==0:
        print("processing:", i_r)
    if row[-1] > 1:
        df_row = pd.DataFrame(row.reshape(1,-1), columns=join_file.columns)
        df_row = pd.concat([df_row]*(int(row[-1])-1), ignore_index=True)
        df_row['start_time'] = np.arange(df_row['start_time'].iloc[0], df_row['start_time'].iloc[0]+WIN*(row[-1]-1), WIN)
        df_row['end_time'] = np.arange(df_row['start_time'].iloc[0] + WIN, df_row['start_time'].iloc[0]+WIN*(row[-1]), WIN)
        join_copy = pd.concat([join_copy, df_row])
        df_row = pd.DataFrame(row.reshape(1,-1), columns=join_file.columns)
        df_row['start_time'] = df_row['start_time'] + WIN*(row[-1]-1)
        if df_row['end_time'].iloc[0]-df_row['start_time'].iloc[0] != 0:
            df_row['cpu_usage'] = df_row['cpu_usage']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['gpu_wrk_util'] = df_row['gpu_wrk_util']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['avg_mem'] = df_row['avg_mem']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['max_mem'] = df_row['max_mem']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['avg_gpu_wrk_mem'] = df_row['avg_gpu_wrk_mem']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['max_gpu_wrk_mem'] = df_row['max_gpu_wrk_mem']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['read'] = df_row['read']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['write'] = df_row['write']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['read_count'] = df_row['read_count']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['write_count'] = df_row['write_count']/(WIN/(df_row['end_time']-df_row['start_time']))
            join_copy = pd.concat([join_copy, df_row])
    if i_r%1000 == 0:
        join_copy.to_csv("joins20/join_single_slot_"+str(i_r)+".csv")
        join_copy = pd.DataFrame(None, columns=join_file.columns)

join_copy.to_csv("joins20/join_single_slot_2.csv")

join_copy['len_inter'] = join_copy['end_time'].values - join_copy['start_time'].values
join_copy['num_slots'] = (join_copy['len_inter'].values // WIN)
join_copy['start_slot'] = (join_copy['start_time'].values - START_TIME) // WIN

join_copy = join_copy.sort_values(by=['start_time'])
join_copy = join_copy.loc[join_copy['num_slots']<=1]
join_copy.to_csv("join_single_slot.csv")
"""

files = sorted(glob.glob("joins20/join_single_*.csv"))
df2 = pd.DataFrame(None, columns=pd.read_csv(files[0]).columns)
for f in files:
    df = pd.read_csv(f)
    df2 = pd.concat([df2, df])

df2 = df2.sort_values(by=['start_time'])
df2['len_inter'] = df2['end_time'].values - df2['start_time'].values
df2['num_slots'] = (df2['len_inter'].values // WIN)
df2['start_slot'] = (df2['start_time'].values - START_TIME) // WIN
df2['slot'] = df2['start_time']//WIN
df2['avgcpu'] = df2['cpu_usage']*df2['len_inter']
df2['avgmem'] = df2['avg_mem']*df2['len_inter']
df2['avggpu'] = df2['gpu_wrk_util']*df2['len_inter']
df2['avggpumem'] = df2['avg_gpu_wrk_mem']*df2['len_inter']
df2['maxmem'] = df2['max_mem']*df2['len_inter']
df2['maxgpumem'] = df2['max_gpu_wrk_mem']*df2['len_inter']

group = df2.groupby(by=['slot']).sum()
group.to_csv("joins20/grjoin20.csv")

# In[44]:


# In[ ]:




