# -*- coding: utf-8 -*-
"""
Created on Thu Feb 14 17:27:40 2019

@author: chen4416
"""
import sys
sys.path.append('<directory path of Import_BSM_AIMSUN.py>')
import numpy as np
import math
import pandas as pd
import sqlite3
import csv
from sqlite3 import Error
import Import_sqlite3 as imSQL
import Parameter_estimation as pe
import matplotlib.pyplot as plt
import matplotlib.animation as animation

global start_time
start_time = 300
global end_time
end_time = 3000
global time_step 
time_step = 6
global link_length
link_length = 1800

def main():
    global counter
    counter = 1
    global cell_length
    cell_length = pe.initialize_ffs()*time_step*5280.0/3600     #in feet
    global cell_number
    cell_number = int(math.ceil(link_length/cell_length))    #in feet
    #all_data = pd.read_csv(address)  use pandas package
    #list(all_data.columns.values)
    boundaries = calculate_bound_locations(link_length, cell_length, cell_number)
    link_lane_number = 3
    
    ####### initialize the Kalman Filter #######    
    ####### set up matrices to store data #######
    penetration_rate = 1.0/11
    total_time_steps = int((end_time - start_time)/time_step)
    KF_occ = np.zeros(shape=(total_time_steps, cell_number))
    mea_occ = np.zeros(shape=(total_time_steps, cell_number))
    mea_occ_via_speed = np.zeros(shape=(total_time_steps, cell_number))
    pre_occ = np.zeros(shape=(total_time_steps, cell_number))
    KF_flow = np.zeros(shape=(total_time_steps, cell_number+1))
    mea_flow = np.zeros(shape=(total_time_steps, cell_number+1))
    KF_speed = np.zeros(shape=(total_time_steps, cell_number))
    pre_speed = np.zeros(shape=(total_time_steps, cell_number))
    KF_capacity = np.zeros(shape=(total_time_steps, 1))
    KF_capacity[0,0] = 1300*link_lane_number
    KF_ffs = np.zeros(shape=(total_time_steps, 1))
    KF_ffs[0,0] = 70
    KF_bws = np.zeros(shape=(total_time_steps, 1))
    KF_bws[0,0] = 16
    average_car_length = pe.get_average_car_length()
    jam_density = int(5280/(average_car_length*1.5))*link_lane_number
    InputFlowRate = 0

    ####### parameters for the kalman filter #######
    Q_occ = 0.2
    Q_cap = 0.1
    Q_ffs = 0.1
    Q_sws = 0.1
    Q_speed = 0.2
    P_cap = 0.8
    P_ffs = 0.8
    P_sws = 0.8
    P_input = 0.9
    R_occ = 0.75
    R_cap = 0.1
    R_ffs = 0.1
    R_sws = 0.1
    R_input = 0.7
    R_speed = 0.7
    KG_cap = 0.5
    KG_ffs = 0.5
    KG_sws = 0.5
    P_occ = np.zeros(shape=(total_time_steps,cell_number))
    P_speed = np.zeros(shape=(total_time_steps,cell_number))
    P_occ[0,:] = [0.9]*cell_number   
    P_speed[0,:] = [0.2]*cell_number   
    address = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/Data_2_vehicle_info.csv"    
    #db_file = "C:\\Users\\Administrator\Dropbox\\BSM Project\\MicroSim_scenario1\\BSM_TSE.sqlite"   
    db_file = "D:\\BSM\\test1_3_13\\probe_vehicles_BSM_3_14.sqlite"
    save_to = "D:\\BSM\\"
    inflow_file = "C:/Users/chen4416.AD/Dropbox/BSM Project/MicroSim_scenario1/actual_inflow.csv"
    ############### make sure the raw data has been ordered by time frames ###############
    actual_inflow = pd.read_csv(inflow_file,header=None)
    
    try:    
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
                    if ((round(t,1) % time_step) == 0): 
                        
                        ##### get estimation from the function
                        ##### get input flow #####
                        if (round(t,1) == start_time):
                            [mean_speeds, max_speeds, probe_traffic_states] = add_data_to_PTS_table_time_space_diagram_method(conn, round(t,1), start_time, boundaries, 3,cell_length)  ## spatial buffer is 5 ft                    
                            occ_measurement = probe_traffic_states[0][1:]
                            flow_measurement = probe_traffic_states[1][:]   
                            mea_occ[0][:] = occ_measurement
                            mea_flow[0][:] = flow_measurement
                            mea_occ_via_speed[0][:] = occ_measurement
    
                            InputFlowRate = flow_measurement[0]/penetration_rate
                            #InputFlowRate = 1/penetration_rate + R_input
                            #InputFlowRate = actual_inflow.iloc[0]
                            KF_occ[0,:] = mea_occ[0][:] 
                            KF_flow[0,:] = mea_flow[0][:]
                            #P_occ[1,:] = P_occ[0,:]
                            
                        else:
                            i = int((round(t,1)-start_time)*1.0/time_step)
                            print("Predict time step: ", i)
                            [mean_speeds, max_speeds, probe_traffic_states] = add_data_to_PTS_table_time_space_diagram_method(conn, round(t,1), start_time, boundaries, 3,cell_length)  ## spatial buffer is 5 ft
                            
                            
                            occ_measurement = probe_traffic_states[0][1:]
                            flow_measurement = probe_traffic_states[1][:]
    
                            mea_occ[i][:] = occ_measurement
                            mea_flow[i][:] = flow_measurement
                            
    
                            InputFlowRate_pre = InputFlowRate
                            InputFlowRate_mea = flow_measurement[0]/penetration_rate
                            #InputFlowRate_mea = 1/penetration_rate
                            KG_input = P_input/(P_input+R_input)
                            P_input = max(0, P_input - KG_input*P_input)
                            InputFlowRate = InputFlowRate_pre + KG_input*(InputFlowRate_mea-InputFlowRate_pre)
                            #InputFlowRate = int(actual_inflow.iloc[i])
                            #print("Time",i,"InputFlowRate: ",InputFlowRate)        
                            print("input flow: ", InputFlowRate)
                            
                            Q_pre = KF_capacity[i-1,0] + rand(Q_cap)  ### use the parameter values in the last time step
                            ffspeed_pre = KF_ffs[i-1,0] + rand(Q_ffs)
                            swspeed_pre = KF_ffs[i-1,0] + rand(Q_sws)
                            
                            if ((mean_speeds.mean() != -1) and (max_speeds.mean() != -1)): 
                                #print("mean speed != -1")
                                #Q_mea = 3600*link_lane_number/pe.get_small_headways(conn, t) + rand(R_cap)
                               
                                #swspeed_mea= KF_bws[i-1][0] + rand(R_sws)
                                
                                low_speed_cell = np.zeros(cell_number)
                                
                                for cell in range(cell_number):  ### to determine in what state the link is 
                                    if ((mean_speeds[cell] < 0.8*ffspeed_pre) and (mean_speeds[cell] > 0)):
                                        low_speed_cell[cell] =1
                                                                   
    
                                if (sum(low_speed_cell) >= 4): ### under congestion condition
                                    print("Estimate shockwave speed first")
                                    #ffspeed_mea = max(max_speeds)
                                    KF_ffs[i,0] = ffspeed_pre
                                    
                                    reaction_time = pe.get_reaction_time(conn,t) 
                                    if (reaction_time > 0):
                                        swspeed_mea = average_car_length*0.6818/reaction_time ########################
                                        #swspeed_mea = (average_car_length+2)*0.6818/0.8 
                                        swspeed_mea =  swspeed_mea
                                        
                                        KG_ffs = P_ffs/(P_ffs+R_ffs)
                                        KG_sws = P_sws/(P_sws+R_sws)                                    
                                        
                                        P_ffs =  P_ffs - KG_ffs*P_ffs
                                        P_sws =  P_sws - KG_sws*P_sws    
    
    
                                        KF_bws[i,0] = swspeed_pre + KG_sws*(swspeed_mea-swspeed_pre)   
                                        
                                        if ( Q_pre > jam_density/((1.0/KF_ffs[i,0])+(1.0/KF_bws[i,0])) ):
                                            KF_capacity[i,0]= jam_density/((1.0/KF_ffs[i,0])+(1.0/KF_bws[i,0]))
                                        else:
                                            KF_capacity[i,0] = Q_pre
                                
                                    else:
    
                                        KF_capacity[i,0] = Q_pre      
                                        KF_bws[i,0] = swspeed_pre
                                        
                                else:
                                    small_headway = pe.get_small_headways(conn, t)
                                    ffspeed_mea = max(max_speeds)
                                    KG_ffs = P_ffs/(P_ffs+R_ffs)
                                    KF_ffs[i,0] = ffspeed_pre + KG_ffs*(ffspeed_mea-ffspeed_pre)   
                                    
                                    if (small_headway >0):
                                        Q_mea = 3600*link_lane_number/small_headway
                                                
                                        KG_cap = P_cap/(P_cap+R_cap)
    
                                        P_cap =  P_cap - KG_cap*P_cap    
                                        P_ffs =  P_ffs - KG_ffs*P_ffs
    
                                        KF_capacity[i,0] = Q_pre + KG_cap*(Q_mea-Q_pre)                                      
                                                                        
                                        if (swspeed_pre < KF_capacity[i,0]/(jam_density-(KF_capacity[i,0]/KF_ffs[i,0] ))  ):
                                            KF_bws[i,0] = KF_capacity[i,0]/(jam_density-(KF_capacity[i,0]/KF_ffs[i,0] ))
                                        else:
                                            KF_bws[i,0] = swspeed_pre
                                    
                                    else:
                                        KF_capacity[i,0] = Q_pre    
                                        KF_bws[i,0] = swspeed_pre
    
    
                            else:
                                KF_capacity[i,0] = Q_pre
                                KF_ffs[i,0] = ffspeed_pre         
                                KF_bws[i,0] = swspeed_pre
                               
        
                        
                            for j in range(cell_number+1):
                                if (j == 0):    
                                    KF_flow[i][j] = InputFlowRate
                                    
                                elif (j == cell_number):
                                    KF_flow[i][j] = min(KF_occ[i-1][j-1], KF_capacity[i][0]*time_step/3600)
                                    KF_flow[i][j] = KF_flow[i][j]
                                    
                                else:
                                    Capacity1 = KF_capacity[i][0]
                                    ffspeed1 = KF_ffs[i][0]
                                    swspeed1 = KF_bws[i][0]
                                    KF_flow[i][j] = min(KF_occ[i-1][j-1], Capacity1*time_step/3600, (ffspeed1*1.0/swspeed1)*(jam_density*cell_length/5280-KF_occ[i-1][j]))
                                    KF_flow[i][j] = KF_flow[i][j]
                                    
        
                            for j in range(cell_number):  ##the ith cell has j = i-1
                                pre_occ[i][j] = KF_occ[i-1][j] + KF_flow[i][j] - KF_flow[i][j+1] + rand(Q_occ)
                                if (pre_occ[i][j]<0):
                                    print ( "Measurement errors: negative occupancy ")
                                if (mean_speeds[j] > 0):                                    
                                    if (mean_speeds[j] > KF_ffs[i][0]*0.8):
                                        print("occ measurement type 1")
                                        critical_occ = KF_capacity[i,0]/KF_ffs[i][0]*0.9
                                        Occ_mea = 0.5*critical_occ*(cell_length/5280) 
                                    elif (mean_speeds[j] > (KF_capacity[i,0]/(jam_density-(KF_capacity[i,0]/KF_bws[i,0])))):
                                        print("occ measurement type 2")
                                        Occ_mea = KF_capacity[i,0]*(cell_length/5280)/mean_speeds[j] 
                                    else:
                                        print("occ measurement type 3")
                                        Occ_mea = jam_density*(cell_length/5280)/(1+(KF_ffs[i][0]/KF_bws[i,0]))
                                else:
                                    print("occ measurement type 4")
                                    Occ_mea = max( 0, KF_occ[i-1][j] + abs(rand(R_occ)) )
                                mea_occ_via_speed[i][j] = Occ_mea
                                #Occ_mea = max(0, occ_measurement[j] + abs(rand(R_occ))) 
                                P_occ[i][j] =  P_occ[i-1][j] + Q_occ
                                if (P_occ[i][j] <0):
                                    print("##########################ERROR################################")
                                KalmanGain = P_occ[i][j]*1.0/(P_occ[i][j]+ R_occ)
                                
                                KF_occ[i][j] =  (1-KalmanGain)*pre_occ[i][j]+ KalmanGain*Occ_mea             
                                P_occ[i][j] = P_occ[i][j] - KalmanGain*P_occ[i][j]
                                
                                ##### calculate KF_speed ####
                                Density = KF_occ[i][j]*5280/cell_length
                                option1 = KF_ffs[i,0]
                                option2 = KF_capacity[i,0]/Density
                                option3 = (jam_density-Density)*KF_bws[i,0]/Density
                                pre_speed[i][j] = min(option1, option2, option3) + rand(Q_speed)
                                mea_speed =  mean_speeds[j]
                                P_speed[i][j] = P_speed[i][j] + Q_speed
                                KG_speed =  P_speed[i][j]/( P_speed[i][j] +R_speed)
                                KF_speed[i][j] = (1-KG_speed)*pre_speed[i][j] + KG_speed*mea_speed
                                P_speed[i][j] = P_speed[i][j] - KG_speed*P_speed[i][j] 
                                #print("Time Step: ", i, " Cell: ", j+1, " Speed: ", KG_speed[i][j])
                                
# =============================================================================
#                         fig  = plt.figure(figsize=(10,3.5),dpi=100)
#                         plt.plot(KF_occ[:, 2],linewidth=0.8)
#                         plt.plot(mea_occ[:, 2]*(1.0),linewidth=0.3)
#                         plt.plot(mea_occ_via_speed[:, 2],linewidth=0.3)
#                         plt.plot(pre_occ[:, 2],linewidth=0.3)
#                         plt.xlabel('time step')
#                         plt.ylabel('occupancy (veh)')
#                         plt.legend(['Estimation','Measured Occ via Occ', 'Measured Occ via Speed', 'Predict Occ via CTM'])
#                         plt.show(fig)
# =============================================================================
            print("all row number: ", counter)
            
            ################################ finish adding new raw data in the database ################################
            ########## to different 0.1 time frame 
            try:
                occ_file = save_to+"\\test2_3_13\\KF_occ_3_12.csv"
                np.savetxt(occ_file, KF_occ,delimiter=',')
                occ_file_mea_speed = save_to+"\\test2_3_13\\KF_occ_mea_via_speed_3_12.csv"
                np.savetxt(occ_file_mea_speed, mea_occ_via_speed,delimiter=',')
                occ_file_predict_CTM = save_to+"\\test2_3_13\\KF_occ_pre_via_CTM_3_12.csv"
                np.savetxt(occ_file_predict_CTM, pre_occ,delimiter=',')
                flow_file = save_to+"\\test2_3_13\\KF_flow_3_12.csv"
                np.savetxt(flow_file,KF_flow,delimiter=',')
                speed_file = save_to+"\\test2_3_13\\pre_speed_3_12.csv"
                np.savetxt(speed_file,pre_speed,delimiter=',')
                speed_file = save_to+"\\test2_3_13\\KF_speed_3_12.csv"
                np.savetxt(speed_file,KF_speed,delimiter=',')                
                parameter_file = save_to+"\\test2_3_13\\parameters_3_12.csv"
                parameters = np.zeros(shape = (total_time_steps, 3))
                parameters[:,0]= KF_capacity[:,0]
                parameters[:,1]= KF_ffs[:,0]
                parameters[:,2]= KF_bws[:,0]
                np.savetxt(parameter_file, parameters,delimiter=',')
                
                for pl in range(cell_number):
                    fig  = plt.figure(figsize=(6,3.5),dpi=100)
                    print("################ Cell",pl+1,"##################")
                    plt.plot(KF_occ[:, pl],linewidth=0.8)
                    plt.plot(mea_occ[:, pl]*(1.0/penetration_rate),linewidth=0.6)
                    plt.xlabel('time step')
                    plt.ylabel('occupancy (veh)')
                    plt.legend(['Estimated Occupancy','Measured Occupancy'])
                    plt.show(fig)
            except Error as e:
                for pl in range(cell_number):
                    if (pl == 2):
                        fig  = plt.figure(figsize=(6,3.5),dpi=100)
                        print("################ Cell",pl+1,"##################")
                        plt.plot(KF_occ[:, pl],linewidth=0.8)
                        plt.plot(mea_occ[:, pl]*(1.0/penetration_rate),linewidth=0.6)
                        plt.xlabel('time step')
                        plt.ylabel('occupancy (veh)')
                        plt.legend(['Estimated Occupancy','Measured Occupancy'])
                        plt.show(fig)
    
    except Error as e: 
        conn.close()
               
def rand(y):
    return np.random.normal(0,y)




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
            sql_select_occ = '''SELECT vehicle_id FROM BSM
            WHERE current_pos_section BETWEEN ? AND ?
            AND time_step_id = ?
            GROUP BY vehicle_id'''
            sql_values = (each_bound-epsilon,each_bound+epsilon,time_step_id)
            if (i==0):
                sql_values = (-5,5,time_step_id)
            cur.execute(sql_select_occ, sql_values)
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
                #print("insert data to PROBE_TRAFFIC_STATE")
            except Error as e:
                print(e) 
        conn.commit()
    
    if ( (((time-START_TIME) % 60.0) ==0)and((time-START_TIME) != 0)):
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
            sql_values_occ = (cell+1,time-60, time)
            cur.execute(sql_get_occ, sql_values_occ)
            getocc = list(cur.fetchall())
            occupancy = len(getocc)
            print("one_min_occ: ",occupancy)
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
    
    return [all_mean_speed, all_max_speed, current_probe_traffic_state]
        

def test_sql_select_occ():
    db_file = "D:\\BSM\\test2_3_13\\probe_vehicles_BSM_3_12.sqlite"
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        sql_get_occ = """
        SELECT vehicle_id FROM BSM
        WHERE cell_id = ? 
        AND simulation_time BETWEEN ? AND ?
        GROUP BY vehicle_id 
        """
        
        sql_values = (2,300,360)
        cur.execute(sql_get_occ, sql_values)
        getdata = list(cur.fetchall())
        print(getdata)
        
    #### get the 
    
def test_sql_select_occ():
    db_file = "D:\\BSM\\Microsim_output_3_8_2019\\Richfield_BSM_20190103_BSMdata_36818_20190305_165124_1.sqlite"
    current_time = 307.0
    cell_number = 7
    current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1))  ### one cell for storing input flow
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()  
        sql_select_occ = '''SELECT cell_id,COUNT(*) FROM vehicle_info
        WHERE simulation_time = ?
        GROUP BY cell_id'''
        sql_values = (round(current_time,1),)
        cur.execute(sql_select_occ, sql_values)
        getdata = list(cur.fetchall())
        for each_item in getdata:
            item_id = each_item[0]
            item_occ = each_item[1]
            current_probe_traffic_state[0][item_id] = item_occ
        print(current_probe_traffic_state)
        
    
def test_sql_select_flow():
    db_file = "D:\\BSM\\Microsim_output_3_8_2019\\Richfield_BSM_20190103_BSMdata_36818_20190305_165124_1.sqlite"
    time_step_id = 1
    link_length = 4650
    cell_length = pe.initialize_ffs()*time_step*5280.0/3600 
    cell_number = int(math.ceil(link_length/cell_length))
    bounds =  calculate_bound_locations(4650, cell_length, cell_number)
    #bounds = [0.0, 651.2, 1302.4, 1953.6, 2604.8, 3256.0, 3907.2, 4558.4, 4650]
    epsilon = 6
    current_probe_traffic_state = np.zeros(shape=(2, cell_number + 1)) 
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()  
        for i in range(len(bounds)):
            each_bound = bounds[i]
            sql_select_occ = '''SELECT vehicle_id FROM vehicle_info
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
