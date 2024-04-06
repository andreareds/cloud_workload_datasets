#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import glob


# In[77]:

df_task = pd.read_csv("taskTotal.csv")


# In[ ]:


def preprocess(x):
    df2=pd.merge(df_task,x, how='inner', left_on=["job ID", "task index"], right_on=["job ID","task index"])
    df2.to_csv("df3.csv",mode="a",header=False,index=False)

reader = pd.read_csv("usageTotal.csv", chunksize=1000) # chunksize depends with you colsize

[preprocess(r) for r in reader]
