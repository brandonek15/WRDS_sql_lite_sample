'''
#this will import the CRSP computstat related data straight from the WRDS server
, do basic cleaning, and put it into the sqlite database.
#Then it will do the same with the FRBNY CRSP/RSSD dataset
#then it will do a complicated join operation to merge on RSSDs onto the permcos
#At the end I will have a dataset with CRSP/Compustat merged with permcos
'''

import pandas as pd
import os
import sqlite3
import wrds
import ibis


#Configurations for WRDS
DB = wrds.Connection(wrds_username='blzborow')
#DB.create_pgpass_file()

#Configurations for my directory
#print(os.getcwd())
#using os.path.join allows for flexibility with both Windows and Linux
ROOT = 'C:\\Users\\Brand\\PycharmProjects\\WRDS_sql_lite_sample\\sql_lite'
SQLITE_FILE = os.path.join(ROOT, 'database_wrds.sqlite')
FINAL_DATA_PATH = os.path.join(ROOT,'final','final_data.csv')

START_DATE = pd.to_datetime('1971-01-01')
END_DATE = pd.to_datetime('2018-12-31')

#Set to 1 if you want all of the raw data to be pulled
PULL_RAW = 0

#Changes directory
os.chdir(ROOT)
#Sets home environmanet
os.environ['HOME'] = ROOT

#Connects to SQLite database
ibis.options.interactive = True

'''
#Exploratory
print(DB.list_libraries())
test_table = DB.get_table('crspm', 'msfhdr', obs=10)
print('hello')
'''

#TODO add all of the other datasets to database
#TODO update merge by using IBIS to merge all of the datasets together.
def main():

    #Create/Connect to the database
    conn = sqlite3.connect(SQLITE_FILE)
    ibis_conn = ibis.sqlite.connect(SQLITE_FILE)
    #Set the cursor
    cur = conn.cursor()
    if PULL_RAW == 1:
        #Add all of the data needed necessary to do the CRSP computstat merge
        get_crsp(DB, conn)

    merge_crsp_comp(ibis_conn)

    #Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print('done')


def get_crsp(wrds,connection):

    #data = wrds.raw_sql('select distinct * from crspm.msfhdr')
    print("Starting to pull CRSP Monthly")
    data = wrds.raw_sql("""
    select distinct * from crspm.msf
    where (date >= '01Jan1971')  
    """)

    data.to_sql("crsp_monthly", connection, if_exists="replace",index = False)

    data_headings = wrds.raw_sql("""
    select distinct hshrcd, hprimexc, begdat, enddat, permno
    from crspm.msfhdr;
    """)

    data_headings.to_sql("crsp_headings",connection, if_exists="replace",index = False)
    print("CRSP Monthly Uploaded to Database")

def merge_crsp_comp(db):
    print("start")
    print(db.list_tables())
    #Load in tables
    crsp_monthly = db.table('crsp_monthly')
    crsp_headings = db.table('crsp_headings')
    #Turn these tables into pandas df objections
    crsp_headings_pd = crsp_headings.execute()
    crsp_monthly_pd = crsp_monthly.execute()
    print(crsp_headings.info())
    print(crsp_headings.hshrcd.mean())
    print(crsp_headings.hshrcd.mean().execute())
    print("finish")

def merge_crsp_comp_read_sql_query(connection):
    print("Starting Merge")
    merged_data = pd.read_sql_query("""
                               select distinct a.*, b.*
                               from crsp_monthly as a,
                               crsp_headings as b
                               on a.permno = b.permno
                               	where a.permno = b.permno
                                and (a.date >= b.begdat)
                                and (a.date <= b.enddat)
                                and (a.date >= '1980-01-01') 
	                            and (a.date <= '2018-12-31');
                                """,
                                connection)
    merged_data.to_csv(FINAL_DATA_PATH, index=False)

if __name__ == '__main__':
    main()