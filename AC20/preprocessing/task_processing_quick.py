#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

TASK_PATH = "pai_task_table.csv"
WIN = 5*60 #5 minutes window

task_columns = ["job_name","task_name","inst_num","status","start_time","end_time","plan_cpu","plan_mem",
                 "plan_gpu","gpu_type"]

task_file = pd.read_csv(TASK_PATH, names=task_columns)

task_file = task_file.loc[task_file['status'] == 'Terminated']

task_file['len_inter'] = task_file['end_time'].values - task_file['start_time'].values
task_file['num_slots'] = (task_file['len_inter'].values // WIN) + 1

total_slots = (task_file['end_time'].max() - task_file['start_time'].min())// WIN + 1
START_TIME = task_file['start_time'].min()
END_TIME = task_file['end_time'].max()

task_copy = task_file.copy()
print("Total rows:", task_file.shape[0])

for i_r, row in enumerate(task_file.values):
    if i_r%100==0:
        print("processing:", i_r)
    if row[-1] > 1:
        df_row = pd.DataFrame(row.reshape(1,-1), columns=task_file.columns)
        df_row = pd.concat([df_row]*(int(row[-1])-1), ignore_index=True)
        df_row['start_time'] = np.arange(df_row['start_time'].iloc[0], df_row['start_time'].iloc[0]+WIN*(row[-1]-1), WIN)
        df_row['end_time'] = np.arange(df_row['start_time'].iloc[0] + WIN, df_row['start_time'].iloc[0]+WIN*(row[-1]), WIN)
        task_copy = pd.concat([task_copy, df_row])
        df_row = pd.DataFrame(row.reshape(1,-1), columns=task_file.columns)
        df_row['start_time'] = df_row['start_time'] + WIN*(row[-1]-1)
        if df_row['end_time'].iloc[0]-df_row['start_time'].iloc[0] != 0:
            df_row['plan_cpu'] = df_row['plan_cpu']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['plan_mem'] = df_row['plan_mem']/(WIN/(df_row['end_time']-df_row['start_time']))
            df_row['plan_gpu'] = df_row['plan_gpu']/(WIN/(df_row['end_time']-df_row['start_time']))
            task_copy = pd.concat([task_copy, df_row])
    if i_r%1000==0:    
        task_copy.to_csv("task_single_slot_temp.csv")
        task_file.drop(task_file.index[i_r], inplace=True)
        task_file.to_csv("task_file_cleaned.csv")
task_copy['len_inter'] = task_copy['end_time'].values - task_copy['start_time'].values
task_copy['num_slots'] = (task_copy['len_inter'].values // WIN)
task_copy['start_slot'] = (task_copy['start_time'].values - START_TIME) // WIN

task_copy = task_copy.sort_values(by=['start_time'])
task_copy = task_copy.loc[task_copy['num_slots']==1]

task_copy.to_csv("task_single_slot.csv")

