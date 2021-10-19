#!/usr/bin/python

import mysql.connector
import sys

'''
    This code creates the database with the tables Trip, Station and Weather. 
    The next step is to load the data into the table 
    and then finally  to execute some question regarding the information of the tables.
'''


USER = "root"
database_name = "BikeRide"
PSW = "***********"

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    for database in databases:
        print(database)

def create_database(cursor):
    cursor.execute("CREATE DATABASE %s" %database_name)

def drop_database(cursor):
    cursor.execute("DROP DATABASE %s" %database_name)


def print_results(query, cursor):
    print(query)
    results = cursor.fetchall()
    for item in results:
        print(item)
    print("Number of results: " + str(len(results)))

if __name__ == '__main__':

	
    # Creation of the database
    my_db = mysql.connector.connect(host = 'localhost', user = USER, passwd = PSW)
    cursor = my_db.cursor()
    show_databases(cursor)

    drop_database(cursor)
    create_database(cursor)
    my_db.close()

    # Connection in the database and creation of the tables
    my_db = mysql.connector.connect(host='localhost', user = USER, passwd = PSW, database = database_name)
    cursor = my_db.cursor()

    cursor.execute("CREATE TABLE Weather(date_w DATE NOT NULL,\
    									max_temp REAL,\
    	    							mean_temp REAL,\
    	    							min_temp REAL,\
    		    						max_visibility_miles REAL,\
    									mean_visibility_miles REAL,\
                                    	min_visibility_miles REAL,\
    									max_wind_speed_mph REAL,\
    									mean_wind_speed_mph REAL,\
    									max_gust_speed_mph REAL,\
    									cloud_cover REAL,\
    									events TEXT,\
    									wind_dir_degrees REAL,\
    									zip_code VARCHAR(256) NOT NULL,\
    									PRIMARY KEY(date_w,zip_code))ENGINE=innodb;"
    )
    cursor.execute("CREATE TABLE Station(station_id SMALLINT NOT NULL,\
    	    							station_name TEXT,\
    									latitude REAL,\
    									longtitude REAL,\
    									dock_count SMALLINT,\
    									city TEXT,\
                                        installation_date DATE,\
    									zip_code VARCHAR(256),\
    									PRIMARY KEY(station_id),\
    									INDEX(zip_code),\
    									FOREIGN KEY(zip_code) REFERENCES Weather(zip_code) ON DELETE CASCADE)ENGINE=innodb;"
    )
    cursor.execute("CREATE TABLE Trip(id INTEGER NOT NULL,\
                                    duration INTEGER,\
    				     			start_time TIMESTAMP,\
    				     			start_station_name TEXT,\
                                    start_station_id SMALLINT,\
    				     			end_time TIMESTAMP,\
    				     			end_station_name TEXT,\
    				     			end_station_id SMALLINT,\
    				     			bike_id SMALLINT,\
    				     			PRIMARY KEY(id),\
    				    			INDEX(start_station_id),\
    				     			INDEX(end_station_id),\
    				     			FOREIGN KEY(start_station_id) REFERENCES Station(station_id) ON DELETE CASCADE,\
    				     			FOREIGN KEY(end_station_id) REFERENCES Station(station_id) ON DELETE CASCADE)ENGINE=innodb;"
    )


    # Load csv files into the tables
    cursor.execute("SET GLOBAL local_infile=1")
    query='LOAD DATA LOCAL INFILE "weather.csv" INTO TABLE Weather FIELDS TERMINATED BY ";" LINES TERMINATED BY "\n"'
    cursor.execute(query)
    query='LOAD DATA LOCAL INFILE "station.csv" INTO TABLE Station FIELDS TERMINATED BY ";" LINES TERMINATED BY "\n"'
    cursor.execute(query)
    query='LOAD DATA LOCAL INFILE "trip.csv" INTO TABLE Trip FIELDS TERMINATED BY ";" LINES TERMINATED BY "\n"'
    cursor.execute(query)
    
    cursor.execute('COMMIT')
    
    #Check if the data successfully loaded into the tables
    print("-------  PRINT DATA -------")
    query  = "SELECT * FROM Trip"
    cursor.execute(query)
    print_results(query, cursor)

    query  = "SELECT * FROM Station"
    cursor.execute(query)
    print_results(query, cursor)

    query  = "SELECT * FROM Weather"
    cursor.execute(query)
    print_results(query, cursor)

    if(len(sys.argv) < 2):
        print("Please insert the correct parameters!!")
        sys.exit(1)
    
    # First Question
    Tbl = sys.argv[1]
    Fld = sys.argv[2]
    val = sys.argv[3]
    query = "SELECT * FROM %s WHERE %s.%s = %s;"
    vals = (Tbl, Tbl, Fld, val)
    cursor.execute(query, vals)
    print_results(query, cursor)
 
    # Second Question
    query = "SELECT Station.city, Station.station_name FROM Station;"
    cursor.execute(query)
    print_results(query, cursor)

    my_db.close()
