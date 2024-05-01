import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
from msTeam import msteam
from logMessage import logMessage as lm

'''TODO 
'''

LOG = r"path"
site = "path"
# df1 = pd.read_excel(r'path"
#df1 = pd.read_excel(r'path"
conn_str = "DRIVER={SQL Server};SERVER=Server";DATABASE=DATABASE"
quoted_conn_str = urllib.parse.quote_plus(conn_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}")
# Path to your certificate file
cert_path = r"path"
key_path = r"path"
try:
    url = "path"
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.get(url, headers=headers, json={}, cert=(cert_path, key_path))  # Use verify for SSL certificate verification

    if response.status_code == 200:
        try:
            response_j = response.json()
            print(response_j)
        except ValueError:
            print("Response is not in valid JSON format.")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")


except Exception as e:
    # msteam(site, "RailCar Data for BNSF is not found for Supply Chain Automation see file bnsf_extract.py: Error: " + str(e),"Trent Radford")
    # lm(LOG, "RailCar Data for BNSF is not found for Supply Chain Automation Error: " + str(e))
    print(str(e))

    


    
# # #print(flattened_data)
try:
    df = pd.json_normalize(response_j['elements'])
    
    # Convert dates and times if necessary
    # Example: Convert 'lastEventDate' from string to datetime
    # df['lastEventDate'] = pd.to_datetime(df['lastEventDate'], format='%m/%d/%Y')
    
    # Upload the DataFrame to your SQL database
    # Replace 'your_table_name' with the actual name of the table you're inserting into
    df.to_sql('BNSF', con=engine, schema='up', if_exists='append', index=False)

  # FILL HERE
except Exception as e:
    msteam(site, "Cannot process JSON or load RailCar Data to MS SQL for Supply Chain Automation. Error: " + str(e), "Trent Radford")
    lm(LOG, "Cannot process JSON or load RailCar Data to MS SQL for Supply Chain Automation. Error: " + str(e))
    
    
   