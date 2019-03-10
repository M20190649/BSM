# -*- coding: utf-8 -*-
"""
Created on Thu Feb 14 17:27:40 2019

@author: chen4416
"""

import numpy as np
import math
import pandas as pd
import sqlite3
import csv
from sqlite3 import Error
import Import_sqlite3 as imSQL


global start_time
start_time = 300
global end_time
end_time = 3000

def main():
    #all_data = pd.read_csv(address)  use pandas package
    #list(all_data.columns.values)
    address = "C:/Users/Administrator/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_revised.csv"    
    #db_file = "C:\\Users\\Administrator\Dropbox\\BSM Project\\MicroSim_scenario1\\BSM_TSE.sqlite"   
    db_file = "D:\\BSM\\BSM_TSE_3_9.sqlite"

    end_time = 3000
    global time_step 
    time_step = 6
    global cell_length
    cell_length = initialize_ffs()*time_step*5280.0/3600     #in feet
    global counter
    counter = 1
    link_length = 4650.0
    global cell_number
    cell_number = int(math.ceil(link_length/cell_length))
    ############### make sure the raw data has been ordered by time frames ###############
    with open(address,'r') as data_csv:
        raw_data = list(csv.reader(data_csv,delimiter=','))
        timelist = np.arange(start_time, end_time, 0.1)
        for t in timelist:  
        #######################################################################    
        ###################### every 0.1 second ###############################
        #######################################################################   
            with sqlite3.connect(db_file) as conn:
                data_to_submit = []
                cur = conn.cursor()
                cur.execute("PRAGMA synchronous = OFF")
                cur.execute("BEGIN TRANSACTION")
                for eachLine in raw_data[counter:]:
                    #print(eachLine[1])
                    simulation_time = float(eachLine[1])
                    #if (round(simulation_time,1) <= round(t,1).item()):                                                
                        #print("simulation_time vs t:", simulation_time,"vs",round(t,1))
                    if (round(simulation_time,1) == round(t,1)):
                        counter = counter+1 ## it is not the problem of sqlite                        
                        vehicle_id = int(eachLine[0])
                        simulation_time = round(float(eachLine[1]),1)
                        section_id = int(eachLine[2])
                        segment_id = int(eachLine[3])
                        lane_number = int(eachLine[4])
                        current_pos_section = round(float(eachLine[5]),3)
                        distance_end_section = round(float(eachLine[6]),3)
                        world_pos_x = round(float(eachLine[7]),3)
                        world_pos_y = round(float(eachLine[8]),3)
                        world_pos_z = round(float(eachLine[9]),3)
                        world_pos_x_rear = round(float(eachLine[10]),3)
                        world_pos_y_rear =  round(float(eachLine[11]),3)
                        world_pos_z_rear = round(float(eachLine[12]),3)
                        current_speed =  float(eachLine[13])
                        distance_traveled = float(eachLine[14]) # in the network
                        section_entrance_time = float(eachLine[15])
                        current_stop_time = float(eachLine[16])
                        speed_drop = 0
                        time_step_id = int(math.ceil((simulation_time-start_time)/time_step))
                        cell_id = int(calculate_cell_id(abs(current_pos_section)))
                        data = (vehicle_id,simulation_time,section_id,segment_id,lane_number,current_pos_section,distance_end_section,\
                                world_pos_x,world_pos_y,world_pos_z,world_pos_x_rear,world_pos_y_rear,world_pos_z_rear,current_speed,\
                                distance_traveled,section_entrance_time,current_stop_time, speed_drop,time_step_id,cell_id)
                        
                        data_to_submit.append(data)
                    else:
                        break
                try:
                    cur.executemany(''' INSERT INTO BSM(vehicle_id,simulation_time,section_id,segment_id,lane_number,
                    current_pos_section, distance_end_section,world_pos_x,world_pos_y,world_pos_z,world_pos_x_rear,world_pos_y_rear,
                    world_pos_z_rear,current_speed,distance_traveled,section_entrance_time,current_stop_time,speed_drop,time_step_id,cell_id)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data_to_submit)  
              
                except Error as e:
                        print(e) 
                conn.commit()
                
                boundaries = calculate_bound_locations(link_length, cell_length, cell_number)
                add_data_to_PTS_table_time_space_diagram_method(conn, round(t,1), start_time, boundaries, 5)  ## spatial buffer is 5 ft
                
                
            
        print("all row number: ", counter)
        ################################ finish adding new raw data in the database ################################
        ########## to different 0.3 time frame 
                 

def initialize_ffs():
    address = "C:/Users/Administrator/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_revised.csv"
    all_data = pd.read_csv(address)
    part_data = all_data.loc[all_data.iloc[:,1]<= (300)+3600]
    ffs = math.floor(max(part_data.iloc[:,13]))
    return ffs

def calculate_cell_id(distance_traveled):
    #param: cell_length, distance_traveled#
    cell_id = math.ceil(distance_traveled/cell_length)
    return cell_id

def calculate_bound_locations(link_length, cell_length, cell_number):
    bounds = []
    for i in range(cell_number):
        bounds.append(round(cell_length*i,1))
    bounds.append(link_length)
    return bounds
        

def add_data_to_PTS_table_time_space_diagram_method(conn, time, START_TIME, boundaries, epsilon):
    '''
    :params: conn: connection; time: current time; START_TIME: simulation start time; 
             boundaries: boundary locations of cells; epsilon: buffer size related with location 
    :output: no output. Extract probe vehicle traffic states and store them to the database
    '''
    if ((time % time_step)==0):
        
        current_time = time
        start_time = START_TIME      
        time_step_id = int((time-start_time)/time_step)
        current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1))  ### one cell for storing input flow
        bounds = boundaries
        cur = conn.cursor()  
        sql_select_occ = '''SELECT cell_id,COUNT(*) FROM BSM
        WHERE simulation_time BETWEEN ? AND ?
        GROUP BY cell_id'''
        sql_values = (round(current_time,1)-0.05,round(current_time,1)+0.05)
        cur.execute(sql_select_occ, sql_values)
        getdata = list(cur.fetchall())
        for each_item in getdata:
            item_id = each_item[0]
            item_occ = each_item[1]
            current_probe_traffic_state[0][item_id] = item_occ

        for i in range(len(bounds)):
            each_bound = bounds[i]
            sql_select_occ = '''SELECT vehicle_id FROM BSM
            WHERE current_pos_section BETWEEN ? AND ?
            AND time_step_id = ?
            GROUP BY vehicle_id'''
            sql_values = (each_bound-epsilon,each_bound+epsilon,time_step_id)
            cur.execute(sql_select_occ, sql_values)
            getdata = list(cur.fetchall())
            current_probe_traffic_state[1][i] = len(getdata)
                
        for i in range(cell_number):
            cell_id = i+1                           
            ##### get speeds                
            sql_select_speed = '''SELECT SUM(current_speed), MAX(current_speed), COUNT(*)
            FROM BSM
            WHERE cell_id = ? 
            AND time_step_id = ?
            '''
            sql_value_speed = (cell_id, time_step_id) 
            outflow = current_probe_traffic_state[1][i]
            inflow = current_probe_traffic_state[1][i-1] 
            occ = current_probe_traffic_state[0][i]
            try:
                cur.execute(sql_select_speed, sql_value_speed)
                getspeed = cur.fetchall()
                #print(getspeed)
                if (getspeed[0][2]  > 0):
                    mean_speed = getspeed[0][0]/getspeed[0][2]*1.0
                    max_speed = getspeed[0][1]
                else:
                    mean_speed = -1
                    max_speed = -1
                #print(mean_speed,max_speed,getspeed[0][2])
                sql_insert = '''INSERT INTO PROBE_TRAFFIC_STATE(time_step_id, cell_id, outflow, inflow, occupancy, mean_speed, max_speed)
                VALUES(?,?,?,?,?,?,?)'''
                insert_values = (time_step_id, cell_id, outflow, inflow, occ, mean_speed, max_speed)
                cur.execute(sql_insert, insert_values) 
                #print("insert data to PROBE_TRAFFIC_STATE")
            except Error as e:
                print(e) 
        conn.commit()
        
        

        
    #### get the 
    
def test_sql_select_occ():
    db_file = "D:\\BSM\\BSM_TSE.sqlite"
    current_time = 306.0
    current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1))  ### one cell for storing input flow
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()  
        sql_select_occ = '''SELECT cell_id,COUNT(*) FROM BSM
        WHERE simulation_time = ?
        GROUP BY cell_id'''
        sql_values = (round(current_time,1),)
        cur.execute(sql_select_occ, sql_values)
        getdata = list(cur.fetchall())
        for each_item in getdata:
            item_id = each_item[0]
            item_occ = each_item[1]
            current_probe_traffic_state[0][item_id] = item_occ
        print(current_probe_traffic_state )
        
    
def test_sql_select_flow():
    db_file = "D:\\BSM\\BSM_TSE.sqlite"
    time_step_id = 1
    bounds = [0.0, 651.2, 1302.4, 1953.6, 2604.8, 3256.0, 3907.2, 4558.4, 4650]
    epsilon = 5
    current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1)) 
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()  
        for i in range(len(bounds)):
            each_bound = bounds[i]
            sql_select_occ = '''SELECT vehicle_id FROM BSM
            WHERE current_pos_section BETWEEN ? AND ?
            AND time_step_id = ?
            GROUP BY vehicle_id'''
            sql_values = (each_bound-epsilon,each_bound+epsilon,time_step_id)
            cur.execute(sql_select_occ, sql_values)
            getdata = list(cur.fetchall())
            current_probe_traffic_state[1][i] = len(getdata)
            print("The bound: ", each_bound)
            print("included vehicle: ",getdata, "flow:", len(getdata))
        print(current_probe_traffic_state)
    

    

def add_data_to_PTS_table(conn, time, START_TIME):  
    if ((time % time_step) == 0): 
        #print("Time,",time)
        start_time = START_TIME
        current_time = time - start_time
        total_time_frames = time_step/0.1
        cur = conn.cursor()
        for i in range(cell_number):
            cell_id = i+1
            time_step_id = int(current_time / time_step)  
            occ = 0
            outflow = 0  ## store outflow
            inflow = 0  ## store inflow
            #### get volumes
            sql_select_occ = '''SELECT vehicle_id, COUNT(*) FROM BSM 
            WHERE cell_id = ? 
            AND time_step_id = ? 
            GROUP BY vehicle_id
            '''
            sql_values = (cell_id, time_step_id)           
            cur.execute(sql_select_occ, sql_values)
            getdata = list(cur.fetchall())
            #print(getdata)
            for each in getdata:
                #print('vehicle_id',each[0],'time_frames',each[1])
                if (each[1] == total_time_frames):
                    occ = occ + 1
                    #occ = occ + each[1]*1.0/total_time_frames
                else:
                    #occ = occ + each[1]*1.0/total_time_frames
                    if (each[1] > total_time_frames*0.5):
                        occ = occ + 1
                    #### get flows
                    #print("partial vehicle: ", each[0], type(each[0]))
                    sql_select_one_vehicle = '''SELECT cell_id FROM BSM 
                    WHERE simulation_time = ? 
                    AND vehicle_id = ?        
                    '''
                    sql_value_1 = (current_time+start_time, each[0]) 
                    cur.execute(sql_select_one_vehicle, sql_value_1)
                    getlocation = cur.fetchone()
                    if (getlocation != None):
                        #print("vehicle:", each[0])
                        #print(cell_id, getlocation[0])
                        if(getlocation[0] == cell_id): 
                            if (each[1] > total_time_frames*0.5):
                                inflow = inflow + 1
                        else:
                            if (each[1] <= total_time_frames*0.5):
                                outflow = outflow + 1     
            ##### get speeds                
            sql_select_speed = '''SELECT SUM(current_speed), MAX(current_speed), COUNT(*)
            FROM BSM
            WHERE cell_id = ? 
            AND time_step_id = ?
            '''
            try:
                cur.execute(sql_select_speed, sql_values)
                getspeed = cur.fetchall()
                #print(getspeed)
                if (getspeed[0][2]  > 0):
                    mean_speed = getspeed[0][0]/getspeed[0][2]*1.0
                    max_speed = getspeed[0][1]
                else:
                    mean_speed = -1
                    max_speed = -1
                #print(mean_speed,max_speed,getspeed[0][2])
                sql_insert = '''INSERT INTO PROBE_TRAFFIC_STATE(time_step_id, cell_id, outflow, inflow, occupancy, mean_speed, max_speed)
                VALUES(?,?,?,?,?,?,?)'''
                insert_values = (time_step_id, cell_id, outflow, inflow, occ, mean_speed, max_speed)
                cur.execute(sql_insert, insert_values) 
                #print("insert data to PROBE_TRAFFIC_STATE")
            except Error as e:
                print(e) 
        conn.commit()


def test_add_data_to_PTS_table():  
    db_file = "D:\\BSM\\BSM_TSE.sqlite"
    with sqlite3.connect(db_file) as conn:
        current_time = 6
        total_time_frames = time_step/0.1
        cur = conn.cursor()
        for i in range(cell_number):
            cell_id = i+1
            time_step_id = int(current_time / time_step)  
            occ = 0
            outflow = 0
            inflow = 0
            sql_select_occ = '''SELECT vehicle_id, COUNT(*) FROM BSM 
            WHERE cell_id = ? 
            AND time_step_id = ? 
            GROUP BY vehicle_id
            '''
            sql_values = (cell_id, time_step_id)           
            cur.execute(sql_select_occ, sql_values)
            getdata = list(cur.fetchall())
            #print(getdata)
            for each in getdata:
                #print('vehicle_id',each[0],'time_frames',each[1])
                if (each[1] == total_time_frames*0.5):
                    occ = occ + 1
                else:
                    if (each[1] > total_time_frames*0.5):
                        occ = occ + 1
                    #print("partial vehicle: ", each[0], type(each[0]))
                    sql_select_one_vehicle = '''SELECT cell_id FROM BSM 
                    WHERE simulation_time = ? 
                    AND vehicle_id = ?        
                    '''
                    start_time = 300
                    sql_value_1 = (current_time+start_time, each[0]) 
                    cur.execute(sql_select_one_vehicle, sql_value_1)
                    getlocation = cur.fetchone()
                    if (getlocation != None):
                        #print("vehicle:", each[0])
                        #print(cell_id, getlocation[0])
                        if(getlocation[0] == cell_id):
                            inflow = inflow + 1
                        else:
                            outflow = outflow + 1
                    
            sql_select_speed = '''SELECT SUM(current_speed), MAX(current_speed), COUNT(*)
            FROM BSM
            WHERE cell_id = ? 
            AND time_step_id = ?
            '''
            cur.execute(sql_select_speed, sql_values)
            getspeed = cur.fetchall()
            mean_speed = getspeed[0][0]/getspeed[0][2]
            max_speed = getspeed[0][1]
            #print(mean_speed,max_speed,getspeed[0][2])
            
            sql_insert = '''INSERT INTO PROBE_TRAFFIC_STATE(time_step_id, cell_id, outflow, inflow, occupancy, mean_speed, max_speed)
            VALUES(?,?,?,?,?,?,?)'''
            insert_values = (time_step_id, cell_id, outflow, inflow, occ, mean_speed, max_speed)
            cur.execute(sql_insert, insert_values)
        conn.commit()

if __name__ == '__main__':
    main()
