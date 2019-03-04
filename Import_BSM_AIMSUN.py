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


def main():
    #all_data = pd.read_csv(address)  use pandas package
    #list(all_data.columns.values)
    address = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_length_4649ft1.csv"    
    #db_file = "C:\\Users\\Administrator\Dropbox\\BSM Project\\MicroSim_scenario1\\BSM_TSE.sqlite"   
    db_file = "D:\\BSM\\BSM_TSE.sqlite"
    start_time = 300
    end_time = 1389.3
    time_step = 6
    global cell_length
    cell_length = initialize_ffs()*time_step*5280.0/3600     #in feet
    counter = 0
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
                        cell_id = int(calculate_cell_id(current_pos_section))
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
            
        print("all row number: ", counter)
        ################################ finish adding new raw data in the database ################################
        ########## to different 0.3 time frame 
            
         
def test_case():
    db_file = "C:/Users/Administrator/Dropbox/BSM Project/MicroSim_scenario1/BSM_TSE.sqlite"
    conn = sqlite3.connect(db_file) 
    cur = conn.cursor()
    data = (1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    cur.execute(''' INSERT INTO BSM(vehicle_id,simulation_time,section_id,segment_id,lane_number,
                        current_pos_section, distance_end_section,world_pos_x,world_pos_y,world_pos_z,world_pos_x_rear,world_pos_y_rear,
                        world_pos_z_rear,current_speed,distance_traveled,section_entrance_time,current_stop_time,speed_drop,cell_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)      
    #conn.commit() ##!!!!!!!!
    conn.close()


def initialize_ffs():
    address = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/Data1_vehicle_info_length_4649ft1.csv"
    all_data = pd.read_csv(address)
    part_data = all_data.loc[all_data.iloc[:,1]<= (300)+3600]
    ffs = math.floor(max(part_data.iloc[:,13]))
    return ffs

def calculate_cell_id(distance_traveled):
    #param: cell_length, distance_traveled#
    cell_id = math.ceil(distance_traveled/cell_length)
    return cell_id
    

                      
def basic_road_parameters(address):
    ffs = 0
    road_length = 0
    road_origin_X = 0
    road_origin_Y = 0
    sampled_id = [1,2,5,8,9]
    sample_road_length = [0]*5
    sample_road_origin_X = [0]*50
    sample_road_origin_Y = [0]*5
    
    with open(address, 'r') as in_file:
        in_file.readline()
        for line in in_file:
            eachLine = line.split(",")
            speed = float(eachLine[5])
            if (speed > ffs):
                ffs = speed
    all_data = pd.read_csv(address)
    for i in range(len(sampled_id)):
        target_id = sampled_id[i]
        all_records = all_data[(all_data['VehID']==target_id)]
        Xs = all_records.iloc[:,3]
        Ys = all_records.iloc[:,4]
        sample_road_length[i] = math.sqrt((Xs.iloc[-1]-Xs.iloc[0])**2+(Ys.iloc[-1]-Ys.iloc[0])**2)/5280   ### mile
        sample_road_origin_X[i] = Xs.iloc[0]
        sample_road_origin_Y[i] = Ys.iloc[0]
    road_length = sum(sample_road_length)*1.0/len(sample_road_length)  
    road_origin_X = sum(sample_road_origin_X)*1.0/len(sample_road_origin_X)
    road_origin_Y = sum(sample_road_origin_Y)*1.0/len(sample_road_origin_Y)
    #print sample_road_length
    return [ffs, road_length, road_origin_X, road_origin_Y]



if __name__ == '__main__':
    main()
