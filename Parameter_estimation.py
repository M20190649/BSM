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
import Import_BSM_AIMSUN as iba
import matplotlib.pyplot as plt




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
    
    

def test_get_bws_regression(estimated_capacity, ffs):
    db_file = "D:\BSM\probe_vehicles_BSM_3_12.sqlite"
    cell_number = 8
    time_step = 6
    speed_threshold = 0.8*ffs
    density_threshold = estimated_capacity*1.0/ffs
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        sql_get_data_point = '''
        SELECT time_step_id, cell_id, outflow, occupancy, mean_speed 
        FROM PROBE_TRAFFIC_STATE 
        WHERE cell_id BETWEEN 1 AND ?
        AND mean_speed < ?
        AND mean_speed > 0
        AND occupancy > ?
        '''
        sql_values = (cell_number-1,speed_threshold, 4) ### why it should be tuple even it only needs one parameter
        cur.execute(sql_get_data_point, sql_values)
        df = pd.DataFrame(list(cur.fetchall()))
        df.columns = ['time_step_id', 'cell_id', 'outflow', 'occupancy', 'mean_speed']
        X = df.iloc[:,3].values*(3600.0/time_step)
        Y = df.iloc[:,2].values*(3600.0/time_step)
        reg = np.polyfit(X,Y,deg = 1)
        slope = reg[0]
        if slope > -10:
            print("bad estimated slope", slope)
        else:
            return slope


def identify_speed_drop(vehicle_id):
    db_file = "D:\\BSM\\Microsim_output_3_8_2019\\BSM_TSE2.sqlite"
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        sql_get_trajectory = '''SELECT simulation_time,current_pos_section FROM BSM
        WHERE vehicle_id = ?
        '''
        value_id = (vehicle_id,)
        cur.execute(sql_get_trajectory,value_id)
        trajectory = pd.DataFrame(list(cur.fetchall()))
        #print(trajectory)
        #print(trajectory.iloc[:,0])
        #print(trajectory.iloc[:,1])
        fig  = plt.figure(figsize=(6,6),dpi=100)
        plt.plot(trajectory.iloc[:,0],trajectory.iloc[:,1])
        plt.show(fig)
       # is_speed_drop = 0
    #return is_speed_drop
    

def plot_trajectory_vehicles():
    db_file = "probe_vehicles_BSM_3_12.sqlite"    
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        sql_get_vehicles = '''SELECT vehicle_id FROM BSM
        WHERE simulation_time BETWEEN ? AND ?
        AND lane_number = 3
        '''
        time_range = (0,500)
        cur.execute(sql_get_vehicles, time_range)
        vehicles = np.unique(pd.DataFrame(list(cur.fetchall())))
        del cur
        print(len(vehicles))
        fig  = plt.figure(figsize=(12,5),dpi=100)
        cur = conn.cursor()
        for vehicle_id in vehicles:                
            sql_get_trajectory = '''SELECT simulation_time, current_pos_section FROM BSM
            WHERE vehicle_id = ?
            AND lane_number = 3
            '''
            cur.execute(sql_get_trajectory, (int(vehicle_id),) )
            trajectory = pd.DataFrame(list(cur.fetchall()))
            plt.plot(trajectory.iloc[:,0],trajectory.iloc[:,1],linewidth=0.5)
        plt.show(fig)
    
def get_small_headways(conn, current_time):
    db_file = "D:\BSM\probe_vehicles_BSM_3_12.sqlite"  
    time_range = (current_time-6,  current_time)
    min_headways = []
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()    
        sql_get_small_headways = '''SELECT simulation_time, lane_number, vehicle_id, current_pos_section, current_speed
        FROM BSM
        WHERE simulation_time BETWEEN ? AND ?
        AND lane_number = ?
        ORDER BY
        simulation_time ASC,
        current_pos_section ASC;
        '''
        for lane in range(1,4):
            values = (time_range[0], time_range[1],lane)
            cur.execute(sql_get_small_headways, values)
            to_select = pd.DataFrame(list(cur.fetchall()))
            if to_select.empty:
                continue
            else:
                to_select.columns = ['simulation_time','lane_number','vehicle_id','current_pos_section', 'current_speed']    
                all_headways = []
                for i in range(1,len(to_select)):
                    if (to_select['simulation_time'].iloc[i] == to_select['simulation_time'].iloc[i-1]):
                        front_position = to_select['current_pos_section'].iloc[i]
                        back_position = to_select['current_pos_section'].iloc[i-1]
                        back_speed = to_select['current_speed'].iloc[i-1]
                        one_headway = (front_position - back_position)/back_speed
                        if (one_headway <= 3):
                            all_headways.append(one_headway)
                if (len(all_headways) != 0):            
                    min_headways.append(min(all_headways))
    if (len(min_headways) != 0):  
        return sum(min_headways)/len(min_headways)
    else:
        return -1
            
def get_speed_drop(conn, current_time, vehicle_id, current_speed):
    is_speed_drop = 0
    cur = conn.cursor()
    sql_get_speeds = '''SELECT speed
    FROM BSM
    WHERE simulation_time BETWEEN ? AND ?
    AND vehicle_id = ?
    '''
    sql_values = (current_time, current_time-20*0.1, vehicle_id) 
    cur.execute(sql_get_speeds,sql_values)
    speed_related = pd.DataFrame(list(cur.fetchall()))
    mean_previous_speed = speed_related.mean()
    if (current_speed <= 0.85*mean_previous_speed):
        is_speed_drop = 1
    return is_speed_drop
    
    
def get_reaction_time(conn, current_time):
    
    return get_small_headways(conn, current_time)

def get_average_car_length():
    address = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_static_revised.csv"
    all_static_info = pd.read_csv(address)
    car_lengths =  all_static_info['vehicle_length']
    average_car_length = car_lengths.mean()
    return average_car_length
    
def initialize_ffs():
    address = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_revised.csv"
    all_data = pd.read_csv(address)
    part_data = all_data.loc[all_data.iloc[:,1]<= (300)+3600]
    ffs = math.floor(max(part_data.iloc[:,13]))
    return ffs

