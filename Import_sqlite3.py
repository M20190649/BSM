 # -*- coding: utf-8 -*-
"""
Created on Fri Mar 01 14:53:15 2019

@author: chen4416
"""

import sqlite3
from sqlite3 import Error
 

def sql_statement_BSM_table():
    '''
    Store and return SQL code to generate a table for BSM raw data in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS BSM(
                vehicle_id INT,
                simulation_time decimal,
                section_id INT,
                segment_id INT,
                lane_number INT,
                current_pos_section REAL,
                distance_end_section REAL,
                world_pos_x REAL,
                world_pos_y REAL,
                world_pos_z REAL,
                world_pos_x_rear REAL,
                world_pos_y_rear REAL,
                world_pos_z_rear REAL,
                current_speed REAL,
                distance_traveled REAL,
                section_entrance_time REAL,
                current_stop_time REAL,
                speed_drop boolean,
                time_step_id INT,
                cell_id INT);'''
    return statement

def sql_statement_cumulative_count(cell_number):
    '''
    store and return SQL code to generate a table to store the cumalative counts of vehicles
    :param: cell_length
    :return: statement: SQL code
    '''
    string_all_cells = "time_step_id INT,"
    for i in range(cell_number+1):
        string_all_cells = string_all_cells + "bound_"+str(i)+ " INT"
        if (i < (cell_number)):
            string_all_cells = string_all_cells + ","
            
    statement = "CREATE TABLE IF NOT EXISTS CUMUL_COUNT({0});".format(string_all_cells)
    return statement

def sql_statement_probe_state_table():
    '''
    Store and return SQL code to generate a probe state table in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS PROBE_TRAFFIC_STATE(
                time_step_id INT,
                cell_id INT,
                outflow REAL,
                inflow REAL,
                occupancy REAL,
                mean_speed REAL,
                max_speed REAL);'''
        
    return statement     

def sql_statement_probe_state_time_space_table():
    '''
    Store and return SQL code to generate a probe state table in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS PROBE_TRAFFIC_STATE_TS(
                time_step_id INT,
                cell_id INT,
                flow REAL,
                density REAL,
                space_mean_speed REAL);'''
        
    return statement   

def sql_statement_parameter_table():
    '''
    Store and return SQL code to generate a table for traffic flow parameters in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS PARAMETERS(
                time_step_id INT,
                cell_id INT,
                capacity REAL,
                ffs REAL,
                sws REAL,
                jam REAL);'''
        
    return statement  

def sql_statement_1min_table():
    '''
    Store and return SQL code to generate a table for traffic flow parameters in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS ONE_MIN_STATES(
                one_min_id INT,
                cell_id INT,
                occ REAL,
                flow REAL,
                mean_speed REAL,
                max_speed REAL);'''
        
    return statement  

def sql_statement_1min_table_new():
    '''
    Store and return SQL code to generate a table for traffic flow parameters in sqlite file
    :param: None
    :return: statement: SQL code
    '''
    statement = '''CREATE TABLE IF NOT EXISTS ONE_MIN_STATES_NEW(
                one_min_id INT,
                cell_id INT,
                density REAL,
                flow REAL,
                mean_space_speed REAL);'''
        
    return statement  

def create_table_BSM_data(db_file):
    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_BSM_table()
        #print st
        c.execute(st)
        conn.close()    
    except Error as e:
        print e
    
def create_table_traffic_state(db_file):

    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_probe_state_table()
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)

def create_table_traffic_state_time_space(db_file):

    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_probe_state_time_space_table()
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)        

def create_table_parameters(db_file):
    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_parameter_table()
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)
        
def create_table_cumulative_count(db_file, cell_number):
    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_cumulative_count(cell_number)
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)   
        
def create_table_one_min_states(db_file):
    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_1min_table()
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)   

def create_table_one_min_states_new(db_file):
    try:
        conn = create_connection(db_file)
        c = conn.cursor()
        st = sql_statement_1min_table_new()
        c.execute(st)
        conn.close()
    except Error as e:
        print(e)   
   
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None


def remove_connection(conn):
    """
    :para conn: the Connection object
    close a database connect
    """
    try:       
        conn.close()
    except Error as e:
        print(e)
        
    

def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM vehicle_info")
 
    rows = cur.fetchall()
 
    for row in rows:
        print(row)
 
 
def select_vehicle_by_ID(conn, Vehicle_ID):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT distance_end_section FROM vehicle_info WHERE vehicle_id=?", (Vehicle_ID,))
 
    rows = cur.fetchall()
 
    #for row in rows:
        #print(row)
    return rows   

def create_all_database_tables(db_file):
    create_table_BSM_data(db_file) 
    create_table_traffic_state(db_file)
    create_table_one_min_states(db_file)
    create_table_traffic_state_time_space(db_file)
    create_table_one_min_states_new(db_file)
    #create_table_parameters(db_file)    
    #create_table_cumulative_count(db_file, 8)
    
 
def main():
    #database = "D:\\BSM\\RichfieldSimulation\\Richfield_BSM_20190103_BSMdata_36818_20190125_130726.sqlite"
    db_file = "D:\\BSM\\test2_3_20\\probe_vehicles_BSM_3_20.sqlite"
    create_all_database_tables(db_file)
 
if __name__ == '__main__':
    main()