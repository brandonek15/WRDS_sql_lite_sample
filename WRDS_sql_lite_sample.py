'''
#this will import the CRSP computstat related data straight from the WRDS server
, do basic cleaning, and put it into the sqlite database.
#At the end I will have a dataset with CRSP/Compustat merged
'''

import pandas as pd
import os
import sqlite3
import wrds
import ibis
import numpy as np

#Configurations for WRDS
DB = wrds.Connection(wrds_username='zborowsk')
#Uncomment next line to create pgpass file
#DB.create_pgpass_file()

#Configurations for my directory
#using os.path.join allows for flexibility with both Windows and Linux
#ROOT = 'C:\\Users\\Brand\\PycharmProjects\\WRDS_sql_lite_sample'
ROOT = '/home/blz782/PycharmProjects/WRDS_sample'
SQLITE_FILE = os.path.join(ROOT,'sql_lite', 'database_wrds.sqlite')
PICKLE_PATH = os.path.join(ROOT,'intermediate','post_merge.pkl')
FINAL_DATA_PATH = os.path.join(ROOT,'sql_lite','final','crsp_compustat_merged.csv')
FINAL_TICKER_PATH = os.path.join(ROOT,'sql_lite','final','crsp_tickers_2018Q4.csv')

START_DATE = pd.to_datetime('1971-01-01')
END_DATE = pd.to_datetime('2018-12-31')

#Set to 1 if you want all of the raw data to be pulled
PULL_RAW = 0
MERGE_CRSP_COMPUSTAT = 1
GET_TICKERS = 1

#Changes directory
os.chdir(ROOT)
#Sets home environmanet
os.environ['HOME'] = ROOT

#Create directories if they don't exist
if not os.path.exists(os.path.join(ROOT,'sql_lite')):
    os.mkdir(os.path.join(ROOT,'sql_lite'))
if not os.path.exists(os.path.join(ROOT,'intermediate')):
    os.mkdir(os.path.join(ROOT,'intermediate'))
if not os.path.exists(os.path.join(ROOT,'sql_lite','final')):
    os.mkdir(os.path.join(ROOT,'sql_lite','final'))

#Browse databases
'''
print(DB.list_libraries())
print(DB.list_tables(library="crspm"))
print(DB.describe_table(library="crspm", table="mseall"))
print("hello")
'''

COMP_VARS = ['datadate', 'fyearq', 'fqtr', 'rdq', 'atq', 'aul3q',
             'aol2q','capr1q','seqq','ceqq','chq','cheq','ltq',
             'deracq','deraltq','derlcq','derlltq','derhedglq',
             'dlcq','dlttq','dvpq','prcraq','cshopq','cshoq',
             'mkvaltq','epspiq','epspxq','epsf12','gdwlq','pstkq',
             'dvpspq','pstkrq','oibdpq','oiadpq','nopiq','dpq',
             'niq','piq','ibq','nimq','niitq','xintq','revtq',
             'cogsq','xsgaq','tieq','tiiq','txdiq','txdbaq',
             'txpq', 'txtq', 'txditcq', 'costat', 'capxy', 'intanq',
             'prstkcy', 'aqcy', 'dvy', 'gvkey']

def main():

    if PULL_RAW == 1:
        pull_raw()

    client = create_client()

    if MERGE_CRSP_COMPUSTAT ==1:

        # create each base expression
        crsp_comp = merge_crsp_comp(client)

        #Execute executes the query
        print("Beginning to execute query")
        crsp_comp_pd = crsp_comp.execute()
        print("Beginning to clean CCM data")
        cleaned_ccm = clean_ccm_merged(crsp_comp_pd)

        print(cleaned_ccm.columns)
        cleaned_ccm.to_csv(FINAL_DATA_PATH, index=False)

    if GET_TICKERS == 1:
        get_tickers(client)

def merge_crsp_comp(client):
    '''Merges crsp_compustat data'''
    print("starting to merge CRSP and Compustat data")

    #Load in tables
    crsp_monthly = client.table('crsp_monthly')
    crsp_headings = client.table('crsp_headings')
    crsp_hist_codes = client.table('crsp_hist_codes')
    ccm_link = client.table('ccm_linking_table')
    comp_quarter = client.table('comp_quarter')
    comp_bank_quarter = client.table('comp_bank_quarter')
    comp_annual = client.table('comp_annual')
    comp_current_sic = client.table('comp_current_sic')

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

    #crsp historical share, sic, and exchange codes;
    joined = joined.left_join(crsp_hist_codes, [
        crsp_monthly['permno'] == crsp_hist_codes['permno'],
        crsp_monthly['date'] == crsp_hist_codes['date']
    ])

    #Merge on the gvkey
    joined = joined.left_join(ccm_link,[
        crsp_monthly['permno'] == ccm_link['lpermno'],
        (crsp_monthly['date'] >= ccm_link['linkdt']).fillna(True),
        (crsp_monthly['date'] <= ccm_link['linkenddt']).fillna(True)
    ])
    #Merge on compustat data
    joined = joined.left_join(comp_quarter,[
        ccm_link['gvkey'] == comp_quarter['gvkey'],
        crsp_monthly['date'].year() == comp_quarter['rdq'].year(),
        crsp_monthly['date'].month() == comp_quarter['rdq'].month()
    ])
    #merge on compustat bank data
    joined = joined.left_join(comp_bank_quarter,[
        comp_quarter['gvkey'] == comp_bank_quarter['gvkey'],
        crsp_monthly['date'].year() == comp_bank_quarter['rdq'].year(),
        crsp_monthly['date'].month() == comp_bank_quarter['rdq'].month()
    ])
    #Merge on annual compustat data (Will need to adjust timing!)
    joined = joined.left_join(comp_annual,[
        comp_quarter['gvkey'] == comp_annual['gvkey'],
        crsp_monthly['date'].year() == comp_annual['datadate'].year(),
    ])
    #Merge on current SIC code
    joined = joined.left_join(comp_current_sic,[
        comp_quarter['gvkey'] == comp_current_sic['gvkey']
    ])

    #Keep all variables from crsp_monthly and those specifed from headers (note that I can't have permno twice!
    final_merge = joined[crsp_monthly,
                         crsp_headings['begdat'], crsp_headings['enddat'],
                         crsp_headings['hshrcd'], crsp_headings['hprimexc'],
                         crsp_hist_codes['shrcd'], crsp_hist_codes['siccd'],
                         crsp_hist_codes['exchcd'], crsp_hist_codes['primexch'],
                         ccm_link['gvkey'], ccm_link['linkdt'],
                         ccm_link['linkenddt'], ccm_link['linktype'],
                         comp_quarter['datadate'], comp_quarter['fyearq'],
                         comp_quarter['fqtr'], comp_quarter['rdq'],
                         comp_quarter['atq'], comp_quarter['aul3q'],
                         comp_quarter['aol2q'], comp_quarter['costat'],
                         comp_quarter['capr1q'], comp_quarter['seqq'],
                         comp_quarter['ceqq'], comp_quarter['chq'],
                         comp_quarter['cheq'], comp_quarter['ltq'],
                         comp_quarter['deracq'], comp_quarter['deraltq'],
                         comp_quarter['derlcq'], comp_quarter['derlltq'],
                         comp_quarter['derhedglq'], comp_quarter['dlcq'],
                         comp_quarter['dlttq'], comp_quarter['dvpq'],
                         comp_quarter['prcraq'], comp_quarter['cshopq'],
                         comp_quarter['cshoq'], comp_quarter['mkvaltq'],
                         comp_quarter['epspiq'], comp_quarter['epspxq'],
                         comp_quarter['epsf12'], comp_quarter['gdwlq'],
                         comp_quarter['pstkq'], comp_quarter['dvpspq'],
                         comp_quarter['pstkrq'], comp_quarter['oibdpq'],
                         comp_quarter['oiadpq'], comp_quarter['nopiq'],
                         comp_quarter['dpq'], comp_quarter['niq'],
                         comp_quarter['piq'], comp_quarter['ibq'],
                         comp_quarter['nimq'], comp_quarter['niitq'],
                         comp_quarter['xintq'], comp_quarter['revtq'],
                         comp_quarter['cogsq'], comp_quarter['xsgaq'],
                         comp_quarter['tieq'], comp_quarter['tiiq'],
                         comp_quarter['txdiq'], comp_quarter['txtq'],
                         comp_quarter['txpq'], comp_quarter['txdbaq'],
                         comp_quarter['txditcq'], comp_quarter['capxy'],
                         comp_quarter['intanq'], comp_quarter['prstkcy'],
                         comp_quarter['aqcy'], comp_quarter['dvy'],
                         comp_bank_quarter['dptcq'],
                         comp_annual['datadate'].name('datadate_annual'),
                         comp_annual['sich'].name('sich_annual'),
                         comp_annual['pstkrv'].name('pstkrv_annual'),
                         comp_annual['pstkl'].name('pstkl_annual'),
                         comp_annual['capx'].name('capx_annual'),
                         comp_annual['wcapch'].name('wcapch_annual'),
                         comp_annual['sstk'].name('sstk_annual'),
                         comp_annual['dv'].name('dv_annual'),
                         comp_annual['indfmt'].name('indfmt_annual'),
                         comp_current_sic['sic'].name('sicc')
    ]

    return final_merge

def pull_raw():
    '''Pulls raw data from WRDS and uploads it SQL lite database'''
    # Create/Connect to the database
    conn = sqlite3.connect(SQLITE_FILE)
    # Set the cursor
    cur = conn.cursor()

    # Add all of the data needed necessary to do the CRSP computstat merge

    retrieve_table(DB,conn,'crspq','msf','crsp_monthly')

    retrieve_table(DB, conn, 'crspq', 'msfhdr', 'crsp_headings',
        columns_to_pull=['hshrcd', 'hprimexc', 'begdat', 'enddat', 'permno'])

    retrieve_table(DB,conn,'crspq','mseall','crsp_hist_codes',
        columns_to_pull=['shrcd', 'siccd', 'exchcd', 'primexch', 'permno','date','ticker','cusip'])
    
    #Believe that I should be pulling from compd instead of comp
    retrieve_table(DB, conn, 'comp', 'fundq', 'comp_quarter',
                   columns_to_pull = COMP_VARS)

    retrieve_table(DB, conn, 'comp_bank', 'bank_fundq', 'comp_bank_quarter',
                   columns_to_pull=['gvkey','dptcq','rdq'])
    
    # Believe that I should be pulling from compd instead of comp
    retrieve_table(DB, conn, 'comp', 'funda', 'comp_annual',
                   columns_to_pull=['gvkey','datadate','sich',
                    'pstkrv','pstkl','capx','wcapch','sstk','dv','indfmt'])

    retrieve_table(DB, conn, 'crsp_q_ccm', 'ccmxpf_lnkhist', 'ccm_linking_table',
                   columns_to_pull=['gvkey', 'lpermno','linkdt','linkenddt','linktype'])

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
    #For testing, set to 10000
    #ibis.options.sql.default_limit = 10000
    return ibis.sqlite.connect(SQLITE_FILE)
    print("hell0")

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

def clean_ccm_merged(df):
    """Does the cleaning of the dataframe that I couldn't in IBES"""
    #This includes changing the timing of Compustat Data

    #Create exchange categorical variables
    for var in ['primexch','hprimexc']:
        df['temp'] = np.nan
        df.loc[df[var]=='N','temp'] = 1
        df.loc[df[var]=='A','temp'] = 2
        df.loc[df[var]=='Q','temp'] = 3
        df.loc[(df[var]!='N') & (df[var]!='A') &
           (df[var]!='Q') & (df[var]!=np.nan),'temp'] =4
        df = df.drop(var, axis =1)
        df = df.rename(columns = {'temp':var})

    #Drop observations that aren't in the correct link types
    df = df[(df['linktype']=='LU') | (df['linktype']=='LC')
            | (df['linktype']=='LS')]

    #Turn costat into a binary variable
    df['temp'] = np.nan
    df.loc[df['costat']=='A','temp'] = 1
    df.loc[df['costat']=='I','temp'] = 0
    df = df.drop('costat', axis=1)
    df = df.rename(columns={'temp': 'costat'})

    #Drop link related variables/ unwanted variables
    df =df.drop(columns = ['linkdt','linkenddt',
        'linktype','begdat','enddat','indfmt_annual'], axis = 1)

    #Deal with timing issues/ back filling.
    df.sort_values(by=['permno','date'])

    #forward fill so every observation CRSP observation has an older compustat observation
    df_filled = df.groupby(['permno'], as_index=False).fillna(method='ffill')
    #Deal with CRSP observations with same month as compustat observations
    for var in COMP_VARS:
        if var != 'rdq' and var != 'datadate':
            df_filled.loc[df_filled['date'].dt.month==df_filled['rdq'].dt.month,var] = np.nan

    #Now forward fill again
    df.sort_values(by=['permno', 'date'])
    df_filled = df_filled.groupby(['permno'], as_index=False).fillna(method='ffill')

    #now replace
    for var in COMP_VARS:
        df = df.drop(columns = var, axis = 1)
        df[var] = df_filled[var]

    #TODO make the annual data match up

    return df

def get_tickers(client):
    crsp_hist_codes = client.table('crsp_hist_codes')
    crsp_hist_codes = crsp_hist_codes[crsp_hist_codes['date'] == END_DATE]
    final_tickers = crsp_hist_codes[crsp_hist_codes['ticker'],crsp_hist_codes['cusip'],crsp_hist_codes['date']].distinct()
    tickers_pd = final_tickers.execute()
    tickers_pd = tickers_pd.dropna(subset=['cusip'])
    tickers_pd.to_csv(FINAL_TICKER_PATH, index=False)
    print("finished exporting tickers")

if __name__ == '__main__':
    main()