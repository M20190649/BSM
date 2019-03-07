# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 22:09:57 2019

@author: Administrator
"""

import numpy as np
import math
import pandas as pd
import sqlite3
import csv
from sqlite3 import Error
import Import_sqlite3 as imSQL

db_file = "D:\\BSM\\BSM_TSE.sqlite"



def get_bws_regression(conn,cell_number,time_step, speed_threshold, density_threshold):
    ''': params: the database connection
    '''
    cur = conn.cursor()
    sql_get_data_point = '''SELECT time_step_id, cell_id, outflow, occupancy, mean_speed F
    ROM PROBE_TRAFFIC_STATE 
    WHERE cell_id BETWEEN 1 AND ?
    AND time_step_id BETWEEN 
    '''
    selected_cell_id = (cell_number-1,) ### why it should be tuple even it only needs one parameter
    cur.execute(sql_get_data_point, selected_cell_id)
    df = pd.DataFrame(list(cur.fetchall()))
    df.columns = ['time_step_id', 'cell_id', 'outflow', 'occupancy', 'mean_speed']
    ###
    ### try to find out the qualified data points representing the congested condition ###
    ### use speed threshold and density threshold
    ### 
    X = df[(df['occupancy']>0) & (df['occupancy']<density_threshold) & (df['mean_speed'] < speed_threshold)].iloc[:,3].values*(3600.0/time_step)
    Y = df[(df['occupancy']>0) & (df['occupancy']<density_threshold) & (df['mean_speed'] < speed_threshold)].iloc[:,2].values*(3600.0/time_step)
    reg = np.polyfit(X,Y,deg = 1)
    slope = reg[0]
    if slope > -10:
        print("bad estimation")
    else:
        return slope
    
    

def test_get_bws_regression():
    db_file = "D:\\BSM\\BSM_TSE.sqlite"
    cell_number = 8
    time_step = 6
    speed_threshold = 
    density_threshold
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        sql_get_data_point = "SELECT time_step_id, cell_id, outflow, occupancy, mean_speed FROM PROBE_TRAFFIC_STATE WHERE cell_id BETWEEN 1 AND ?"
        selected_cell_id = (cell_number-1,) ### why it should be tuple even it only needs one parameter
        cur.execute(sql_get_data_point, selected_cell_id)
        df = pd.DataFrame(list(cur.fetchall()))
        df.columns = ['time_step_id', 'cell_id', 'outflow', 'occupancy', 'mean_speed']
        X = df[(df['occupancy']>0 )& (df['mean_speed']) < 50].iloc[:,3].values*(3600.0/time_step)
        Y = df[(df['occupancy']>0) & (df['mean_speed']) < 50].iloc[:,2].values*(3600.0/time_step)
        reg = np.polyfit(X,Y,deg = 1)
        slope = reg[0]
        if slope > -10:
            print("bad estimated slope", slope)
        else:
            return slope
