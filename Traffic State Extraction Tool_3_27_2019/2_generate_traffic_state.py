# -*- coding: utf-8 -*-
"""
Created on Thu Feb 14 17:27:40 2019

@author: chen4416
"""
import sys
sys.path.append('<directory path of 2_generate_traffic_state.py>')
import numpy as np
import math
import pandas as pd
import sqlite3
import csv
from sqlite3 import Error

global start_time 
start_time = 300   ##### start simulation time 
global end_time
end_time = 3000 ##### end simulation time
global time_step 
time_step = 6
global link_length
link_length = 1798.3 #### link length


def main():
    #################################### dataset to be used ###################################################################    
    address = "D:/BSM/Microsim_output_3_14_2019/data_2_all_equipped_vehicle_info.csv"    
    global cell_length
    cell_length = initialize_ffs(address)*time_step*5280.0/3600     #in feet
    global counter
    counter = 1
    global cell_number
    cell_number = int(math.ceil(link_length/cell_length))    #in feet
    #all_data = pd.read_csv(address)  use pandas package
    #list(all_data.columns.values)
    boundaries = calculate_bound_locations(link_length, cell_length, cell_number)
    link_lane_number = 3  ##### lane number    
    total_time_steps = int((end_time - start_time)/time_step)
    print("boundaries are: ", boundaries)

    #################################### dataset to store the traffic state ###################################################
    #################################### this is the dataset you creat at last step ###########################################  
    db_file = "D:\\BSM\\data_extractiom_4_4\\all_vehicles_BSM_scenario1_4_4_2.sqlite"
     
    with open(address,'r') as data_csv:
        raw_data = list(csv.reader(data_csv,delimiter=','))
        timelist = np.arange(start_time, end_time+0.1, 0.1)
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
                        #speed_drop = pe.get_speed_drop(conn, simulation_time, vehicle_id, current_speed)
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
                
                ##### adjust probe traffic state table #####
                ########start to apply Kalman filter#######

                add_data_to_PTS_table_time_space_diagram_method(conn, round(t,1), start_time, boundaries, 3,cell_length)  ## spatial buffer is 5 ft
                Time_space_data_extraction(conn,  round(t,1), start_time, boundaries, cell_length)   

        print("all row number: ", counter)
        
        ################################ finish adding new raw data in the database ################################


def initialize_ffs(address):
    all_data = pd.read_csv(address)
    part_data = all_data.loc[all_data.iloc[:,1]<= (start_time)+3600]
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
        

def add_data_to_PTS_table_time_space_diagram_method(conn, time, START_TIME, boundaries, epsilon, cell_length):
    '''
    :params: conn: connection; time: current time; START_TIME: simulation start time; 
             boundaries: boundary locations of cells; epsilon: buffer size related with location 
    :output: no output. Extract probe vehicle traffic states and store them to the database
    '''
    if ((time % time_step)==0):
        
        current_time = time
        start_time = START_TIME      
        time_step_id = int((time-start_time)/time_step)
        #print("time_step ", time_step_id)
        current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1))  ### one cell for storing input flow
        bounds = boundaries
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

        for i in range(len(bounds)):
            each_bound = bounds[i]
            sql_select_flow = '''SELECT vehicle_id FROM BSM
            WHERE current_pos_section BETWEEN ? AND ?
            AND time_step_id = ?
            GROUP BY vehicle_id'''
            sql_values = (each_bound-epsilon,each_bound+epsilon,time_step_id)
            if (i==0):
                sql_values = (-5,5,time_step_id)
            cur.execute(sql_select_flow, sql_values)
            getdata = list(cur.fetchall())
            current_probe_traffic_state[1][i] = len(getdata)
        
        all_mean_speed = np.zeros(cell_number)
        all_max_speed = np.zeros(cell_number)
        
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
            #print("outflow ", outflow, " inflow ", inflow)
            occ = current_probe_traffic_state[0][i]
            try:
                cur.execute(sql_select_speed, sql_value_speed)
                getspeed = cur.fetchall()
                #print(getspeed)
                if (getspeed[0][2]  > 0):
                    mean_speed = getspeed[0][0]/getspeed[0][2]*1.0
                    max_speed = getspeed[0][1]
                    
                    all_mean_speed[i] = mean_speed
                    all_max_speed[i] = max_speed
                else:
                    mean_speed =  -1
                    max_speed =  -1

                    all_mean_speed[i] = mean_speed
                    all_max_speed[i] = max_speed                    
                    
                #print(mean_speed,max_speed,getspeed[0][2])
                sql_insert = '''INSERT INTO PROBE_TRAFFIC_STATE(time_step_id, cell_id, outflow, inflow, occupancy, mean_speed, max_speed)
                VALUES(?,?,?,?,?,?,?)'''
                insert_values = (time_step_id, cell_id, outflow, inflow, occ, mean_speed, max_speed)
                cur.execute(sql_insert, insert_values) 
            except Error as e:
                print(e) 
        conn.commit()
    
    if ( (((time-START_TIME) % 60.0) ==0) and ((time-START_TIME) != 0)):
        one_min_state = np.zeros(shape=(4, cell_number))
        one_min_id = (time-START_TIME)/60.0
        cur = conn.cursor()                
        sql_get_occ = """
        SELECT vehicle_id FROM BSM
        WHERE cell_id = ? 
        AND simulation_time BETWEEN ? AND ?
        GROUP BY vehicle_id 
        """
        sql_get_flow = """
        SELECT vehicle_id FROM BSM
        WHERE current_pos_section BETWEEN ? AND ?
        AND simulation_time BETWEEN ? AND ?
        GROUP BY vehicle_id
        """        
        sql_get_speed = '''
        SELECT SUM(current_speed), MAX(current_speed), COUNT(*)
        FROM BSM
        WHERE cell_id = ? 
        AND simulation_time BETWEEN ? AND ?
        '''
        for cell in range(cell_number):
            sql_values_occ = (cell+1,time-0.05, time+0.05)
            cur.execute(sql_get_occ, sql_values_occ)
            getocc = list(cur.fetchall())
            occupancy = len(getocc)
            #print("one_min_occ: ",occupancy)
            one_min_state[0][cell] = occupancy
            
            sql_values_speed = (cell+1, time-60, time)
            cur.execute(sql_get_speed, sql_values_speed)
            getspeed = cur.fetchall()
            if (getspeed[0][2]  > 0):
                mean_speed = getspeed[0][0]/getspeed[0][2]*1.0
                max_speed = getspeed[0][1]
                flow = mean_speed*occupancy*5280/cell_length
            else:
                mean_speed =  -1
                max_speed =  -1                
                flow = 0
            one_min_state[2][cell] = mean_speed
            one_min_state[3][cell] = max_speed
            
            sql_insert_one_min = '''INSERT INTO ONE_MIN_STATES(one_min_id, cell_id, occ, flow, mean_speed, max_speed)
                VALUES(?,?,?,?,?,?)'''
            insert_values_one_min = (one_min_id, cell+1, occupancy*5280/cell_length, flow, mean_speed, max_speed)
            cur.execute(sql_insert_one_min, insert_values_one_min) 
        conn.commit()   

def Time_space_data_extraction(conn, time, START_TIME, boundaries, cell_length):
    '''
    :params: conn: connection; time: current time; START_TIME: simulation start time; 
             boundaries: boundary locations of cells; epsilon: buffer size related with location 
    :output: no output. Vehicle traffic states extracted using Edie's method
    '''
    if ((time % time_step)==0):        
        start_time = START_TIME      
        time_step_id = int((time-start_time)/time_step)
        current_traffic_state = np.zeros(shape=(2, cell_number + 1))  ### one cell for storing input flow
        all_mean_speed = np.zeros(cell_number)
        bounds = boundaries
        cur = conn.cursor()
        data_to_submit = []
        for i in range(cell_number):
            cell_id = i+1
            sql_get_all_vehicles = """
            SELECT vehicle_id, current_pos_section FROM BSM
            WHERE cell_id = ?
            AND time_step_id = ?              
            """
            sql_values = (cell_id, time_step_id)
            cur.execute(sql_get_all_vehicles, sql_values)
            get_data = pd.DataFrame(list(cur.fetchall()))
            get_data.columns = ['vehicle_id','distance']
            list_vehicle_id = get_data.vehicle_id.unique()

            list_travel_distances = []
            list_travel_times = []
            
            for each_vehicle in list_vehicle_id:
                vehicle_trajectory = get_data[get_data['vehicle_id'] == each_vehicle].iloc[:,1]
                get_travel_time = len(vehicle_trajectory)*0.1/3600
                get_distance = (vehicle_trajectory.iloc[-1] - vehicle_trajectory.iloc[0])/5280
                list_travel_distances.append(get_distance)
                list_travel_times.append(get_travel_time)
                
            if(len(list_travel_times)>0):   
                flow = sum(list_travel_distances)/((time_step*1.0/3600)*(cell_length/5280))
                density = sum(list_travel_times)/((time_step*1.0/3600)*(cell_length/5280))
                space_mean_speed = sum(list_travel_distances)/(sum(list_travel_times))
            else:
                flow = 0
                density = 0
                space_mean_speed = -1
            print("TS: ", time, cell_id, flow,density,space_mean_speed)
            data_to_submit.append((time_step_id,cell_id,flow,density, space_mean_speed))
            
        sql_insert_ts_state = '''INSERT INTO PROBE_TRAFFIC_STATE_TS(time_step_id, cell_id, flow, density, space_mean_speed) VALUES(?,?,?,?,?)'''
        cur.executemany(sql_insert_ts_state, data_to_submit)   
        conn.commit()   
        print( ( (((time-START_TIME) % 60.0) ==0)and((time-START_TIME) != 0)))        
    ########## start to calculate one_minute_state #########
    if ((((time-START_TIME) % 60.0) ==0)and((time-START_TIME) != 0)):
        print("calculate one-minute state")
        one_min_id = int((time-START_TIME)*1.0 / 60.0)
        time_step_id = int((time-start_time)/time_step)
        cur = conn.cursor()
        sql_get_from_ts_table = """
        SELECT flow, density FROM PROBE_TRAFFIC_STATE_TS
        WHERE time_step_id BETWEEN ? AND ?
        AND cell_id = ?
        """
        current_time_step = time_step_id
        early_time_step = time_step_id - int(60*1.0/time_step) + 1
        data_to_submit_to = []
        for each_cell in range(cell_number):
            each_cell_id = each_cell + 1
            sql_values = (early_time_step, current_time_step, each_cell_id)
            cur.execute(sql_get_from_ts_table, sql_values)
            get_data = list(cur.fetchall())
            list_flows = [i[0] for i in get_data]
            list_densities = [i[1] for i in get_data]
            agg_flow = sum(list_flows)/len(list_flows)
            agg_density = sum(list_densities)/len(list_densities)
            agg_speed = sum(list_flows)/sum(list_densities)
            data_to_submit_to.append((one_min_id,each_cell_id, agg_density,agg_flow, agg_speed))
            print("1_min: ", current_time_step, one_min_id,each_cell_id, agg_density,agg_flow, agg_speed)
        sql_insert_to_new_one_min_table =  """INSERT INTO ONE_MIN_STATES_NEW(one_min_id, cell_id, density, flow, mean_space_speed) VALUES(?,?,?,?,?)"""
        try:
            cur.executemany(sql_insert_to_new_one_min_table,data_to_submit_to)
        except Error as e:
            print(e) 
        conn.commit()

    if ((time % time_step)==0):     
        return [all_mean_speed, current_traffic_state]
    
if __name__ == '__main__':
    main()
