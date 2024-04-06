#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

instance_cols = ["job_name","task_name","inst_name","worker_name","inst_id","status","start_time","end_time","machine"]
sensor_cols = ["job_name","task_name","worker_name","inst_id","machine","gpu_name","cpu_usage","gpu_wrk_util",
               "avg_mem","max_mem","avg_gpu_wrk_mem","max_gpu_wrk_mem","read","write","read_count","write_count"]

INSTANCE_TABLE = "pai_instance_table.csv"
SENSOR_TABLE = "pai_sensor_table.csv"

instance_file = pd.read_csv(INSTANCE_TABLE, names = instance_cols)
sensor_file = pd.read_csv(SENSOR_TABLE, names = sensor_cols)

instance_file = instance_file.loc[instance_file['status'] == 'Terminated']

join_table = instance_file.merge(sensor_file, on=["job_name","task_name","worker_name","inst_id", "machine"], how='inner')
join_table.to_csv("instance_sensor_join_table.csv")


# In[ ]:




