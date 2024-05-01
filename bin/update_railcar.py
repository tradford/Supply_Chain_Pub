from msTeam import msteam
from logMessage import logMessage as lm
import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine


LOG = r"path"
site = "path"


try:
    df = pd.read_excel(r'path"
    #print(df.head(20))
    df = df.astype('str')
    file_path = r'path"
    df1 = pd.read_excel(file_path, sheet_name='Leased Fleet')
    df1 = df1.astype('str')
except Exception as e:
    msteam(site, "Could not open the MASTER - ESI Leased Rail Fleet.xlsx file on SharePoint. Error: " + str(e),"Trent Radford")
    lm(LOG, "Could not open the MASTER - ESI Leased Rail Fleet.xlsx file on SharePoint. Error: " + str(e))
    
try:
    # ######## Establish a connection to the database

    conn_str = "DRIVER={SQL Server};" + "SERVER=;DATABASE="
    quoted = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")
    df.to_sql("table", con=engine, if_exists="replace", index=False)
    df1.to_sql("table", con=engine, if_exists="replace", index=False)
    
except Exception as e:
    msteam(site, "Could not connect to MS SQL database. See file update_railcar.py for troubleshooting. Error: " + str(e),"Trent Radford")
    lm(LOG, "Could not connect to MS SQL database. Error: " + str(e))
################################### 
