#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 22:37:40 2019

@author: yan
"""

import numpy as np
import pandas as pd




def getdata(Inputfile):
    data = []
    f = open(Inputfile, 'r')
    variablename = f.readline().rstrip().split(',')
    for line in f:
        data.append([int(x) for x in line.rstrip().split(',')])
    return variablename, data

vs, cpt = getdata('output.csv')

vb, d_cpt = getdata('Tom_cpt.csv')     
 
d = dict(d_cpt)


for i in range(len(cpt)): 
    r = int(cpt[i][1])
    if r in d:
        cpt[i][1] = d[r]

df = pd.DataFrame(cpt, columns = ['hadm_id', 'cpt_number', 'flag_af', 'flag_epidural', 'pre_beta', 'post_beta', 'hypotension_flag'])
df_grouped = df.groupby('hadm_id').agg({'cpt_number':'max'})
df_grouped = df_grouped.reset_index()
df_grouped = df_grouped.rename(columns={'cpt_number':'cpt_max'})
df = pd.merge(df, df_grouped, how='left', on=['hadm_id'])
df = df[df['cpt_number'] == df['cpt_max']]

df = df.drop_duplicates(subset = None, keep = 'first', inplace = False)
raw_data = np.array(df)
#uni_data = np.unique(raw_data, axis = 0)
train_set = raw_data[:,[1,2,3,4,5]]

train_data = pd.DataFrame(train_set, columns = ['cpt_number', 'flag_af', 'flag_epidural', 'pre_beta', 'post_beta'])


# start to train 

from pgmpy.estimators import HillClimbSearch
from pgmpy.estimators import BdeuScore
from pgmpy.models import BayesianModel

bdeu = BdeuScore(train_data, equivalent_sample_size= 13)


hc = HillClimbSearch(train_data, scoring_method= bdeu)
best_model = hc.estimate()
print(best_model.edges())





    