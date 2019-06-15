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

'''
#Exploratory
print(DB.list_libraries())
test_table = DB.get_table('crspm', 'msfhdr', obs=10)
print('hello')
'''

#TODO add all of the other datasets to database
#TODO update merge by using IBIS to merge all of the datasets together.
def main():

    if PULL_RAW == 1:
        pull_raw()

    # create each base expression
    client = create_client()
    crsp_comp = merge_crsp_comp(client)
    #Materialize executes the join.
    crsp_comp_pd = crsp_comp.materialize()
    print(crsp_comp_pd.columns)
    print("finish")
    print('done')


def merge_crsp_comp(client):
    '''Merges crsp_compustat data'''
    print("starting to pull CRSP data")

    #Load in tables
    crsp_monthly = client.table('crsp_monthly')
    crsp_headings = client.table('crsp_headings')

    #Keep only observations from the crsp monthly data that are starting in date range
    #Keep only certain range and not null returns
    crsp_monthly = crsp_monthly[crsp_monthly['date'].between(START_DATE, END_DATE)]
    crsp_monthly = crsp_monthly[(crsp_monthly.ret.notnull() & crsp_monthly.prc.notnull())]

    #Join on CRSP Monthly to CRSP Headers
    joined = crsp_monthly.inner_join(crsp_headings,[
        crsp_monthly['permno'] == crsp_headings['permno'],
        (crsp_monthly['date'] >= crsp_headings['begdat']),
        (crsp_monthly['date'] <= crsp_headings['enddat'])
    ])

    #Keep all variables from crsp_monthly and those specifed from headers (note that I can't have permno twice!
    final_dataset = joined[crsp_monthly,crsp_headings['begdat'],crsp_headings['enddat'],
        crsp_headings['hshrcd'],crsp_headings['hprimexc']]

    return final_dataset

def merge_crsp_comp_read_sql_query(connection):
    '''This is a temporary test ot make sure I can pull from the WRDS tables'''
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

def pull_raw():
    '''Pulls raw data from WRDS and uploads it SQL lite database'''
    # Create/Connect to the database
    conn = sqlite3.connect(SQLITE_FILE)
    # Set the cursor
    cur = conn.cursor()
    # Add all of the data needed necessary to do the CRSP computstat merge
    get_crsp(DB, conn)
    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()

def get_crsp(wrds,connection):
    '''Pulls CRSP data from WRDS'''
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

def create_client():
    """Create and configure a database client"""
    ibis.options.interactive = True
    ibis.options.sql.default_limit = None
    return ibis.sqlite.connect(SQLITE_FILE)

if __name__ == '__main__':
    main()