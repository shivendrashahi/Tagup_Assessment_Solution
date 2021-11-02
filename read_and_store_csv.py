import pandas as pd
import db_crednetials as dbc
import sys
import glob 
import sqlalchemy

def create_db_connection(database_username, database_password, database_ip, database_name):
    """ Creates connection to MySQL database
    input : database_username, database_password, database_ip, database_name
    output : connection engine which can be used to store dataframe as table
    """
    print("Connecting to {0} database".format(database_name))
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
    if database_connection:
        print("Connection Successful")
        return database_connection
    else:
        print("Connection Failed! Check Database Credentials")
        sys.exit()
    

def read_and_save_csv(dir_path):
    """ Function to read all csv files inside specified directory
    dir_path : Location of directory where csv files are present
    return : Combined Dataframe
    """
    print("Reading CSV Files")
    files = glob.glob(dir_path + "/*.csv")

    database_connection = create_db_connection(dbc.username, dbc.password, dbc.ip, dbc.dbname)
    for filename in files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df.rename(columns={'Unnamed: 0':'timestamp'}, inplace=True)
        df.to_sql(con=database_connection, name='machines', if_exists='append')
    print("All csv files stored succesfully")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Kindly provide path where csv files are present!!!!!!!")
        sys.exit()
    dir_path = sys.argv[1]
    read_and_save_csv(dir_path)