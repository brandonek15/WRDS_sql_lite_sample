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

comp_vars = ['datadate','fyearq','fqtr','rdq','atq','aul3q',
             'aol2q','capr1q','seqq','ceqq','chq','cheq','ltq',
             'deracq','deraltq','derlcq','derlltq','derhedglq',
             'dlcq','dlttq','dvpq','prcraq','cshopq','cshoq',
             'mkvaltq','epspiq','epspxq','epsf12','gdwlq','pstkq',
             'dvpspq','pstkrq','oibdpq','oiadpq','nopiq','dpq',
             'niq','piq','ibq','nimq','niitq','xintq','revtq',
             'cogsq','xsgaq','tieq','tiiq','txdiq','txdbaq',
             'txpq','txtq','txditcq','costat','capxy','intanq',
             'prstkcy','aqcy','dvy','gvkey']
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

def pull_raw():
    '''Pulls raw data from WRDS and uploads it SQL lite database'''
    # Create/Connect to the database
    conn = sqlite3.connect(SQLITE_FILE)
    # Set the cursor
    cur = conn.cursor()

    # Add all of the data needed necessary to do the CRSP computstat merge

    retrieve_table(DB,conn,'crspm','msf','crsp_monthly')
    
    retrieve_table(DB, conn, 'crspm', 'msfhdr', 'crsp_headings',
        columns_to_pull=['hshrcd', 'hprimexc', 'begdat', 'enddat', 'permno'])
    
    retrieve_table(DB,conn,'crspm','mseall','crsp_hist_codes',
        columns_to_pull=['shrcd', 'siccd', 'exchcd', 'primexch', 'permno','date']     )
    
    retrieve_table(DB, conn, 'compd', 'fundq', 'comp_quarter',
                   columns_to_pull = comp_vars)
    
    retrieve_table(DB, conn, 'comp_bank', 'bank_fundq', 'comp_bank_quarter',
                   columns_to_pull=['gvkey','dptcq','rdq'])
    
    retrieve_table(DB, conn, 'compd', 'funda', 'comp_annual',
                   columns_to_pull=['gvkey','datadate','sich',
                    'pstkrv','pstkl','capx','wcapch','sstk','dv','indfmt'])

    retrieve_table(DB, conn, 'crsp_m_ccm', 'ccmxpf_lnkhist', 'ccm_linking_table',
                   columns_to_pull=['gvkey', 'lpermno','linkdt','linkenddt'])

    retrieve_table(DB, conn, 'comp', 'company', 'comp_current_sic',
                   columns_to_pull=['gvkey', 'sic'])

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()

def create_client():
    """Create and configure a database client"""
    ibis.options.interactive = True
    ibis.options.sql.default_limit = None
    return ibis.sqlite.connect(SQLITE_FILE)

def retrieve_table(wrds,connection,library,table,heading,columns_to_pull = 'all'):
    """Pull the WRDS table using the get_table command and upload to SQL lite database"""
    print("Pulling library: " + library + ", table: " + table)
    if columns_to_pull == 'all':
        wrds_table = wrds.get_table(library, table)
    else:
        wrds_table = wrds.get_table(library, table, columns=columns_to_pull)

    wrds_table.drop_duplicates()
    wrds_table.to_sql(heading, connection, if_exists="replace", index=False)
    print("Finished pulling library: " + library + ", table: " + table)

if __name__ == '__main__':
    main()