import pandas as pd
import os
import oracle_connect2 as oracle_connect
from logMessage import logMessage as lm
from sqlalchemy import create_engine
import os
import requests
from msTeam import msteam
from datetime import datetime as dt

''' TODO: Pull data from excel file and load into database'''
         
#df = pd.read_excel(r'path"

#####SET VARIABLES
today = dt.today()
tody = today.strftime('%b%y')
if tody[0:3] == 'Oct' or tody[0:3] == 'Nov' or tody[0:3] == 'Dec':
    yr = int(tody[3:]) + 1
    sheetnm = '20' + tody[3:] + '-' + str(yr) 
else:
    yrs = int(tody[3:]) -1
    sheetnm = '20' +  str(yrs) + '-' + tody[3:]

#print(sheetnm)
LOG = r"path"
site = "path"

#####GET TOKEN FOR SHAREPOINT

try:
    url_toke = "path"

    payload='payload'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': ''
    }

    response = requests.request("GET", url_toke, headers=headers, data=payload)

    #print(response.text)
    token = response.json()

###### Step 3 Access site and copy RAIL TRANSPORT TASK data to a local folder

    access = "Bearer " + token["access_token"]

    #url = "path"

    url = "path"

    payload={}
    headers = {
    'Authorization': access
    }

    response = requests.request("GET", url, headers=headers, data=payload)

   # print(response.text)
    

    downld = response.json()
    try:
        err = downld['error']['code']
        lm(LOG, "Error Code: " + err)
        
    
        if err == 'itemNotFound':
            msteam(site, "Error: " + downld['error']['message'],"Trent Radford")
            lm(LOG, "Error: " + downld['error']['message'])
    except KeyError as ke:
        msteam(site, "No errors in Railed Transport API call","Trent Radford")
        lm(LOG, "No errors in Railed Transport API call")
        #print(ke)

except Exception as e:
    msteam(site, "Error: " + str(e),"Trent Radford")
    lm(LOG, "Error: " + str(e))
    #print(e)

try:
    downld_id = downld['value'][7]['id']
    if downld_id == '':
        lm(LOG, "Correct file")
        downld_url = downld['value'][7]['@microsoft.graph.downloadUrl']

        cwd = r'path"
        response = requests.get(downld_url)
        os.chdir(cwd)
        filename = ""
        open(filename, "wb").write(response.content)

except Exception as e:
    msteam(site, "Railed Transport Task file is not found for Supply Chain Automation see file do_data_category.py: Error: " + str(e),"Trent Radford")
    lm(LOG, "Railed Transport Task file is not found for Supply Chain Automation Error: " + str(e))

##### Step 3 Read the data into a dataframe
filename = "Railed Transport Tasks.xlsx"
#print(filename)
#print(sheetnm)
df1 = pd.read_excel(rf'path"
##df1.dropna(how='all', axis=1, inplace=True)
#print(df1.head(20))
df1.drop(columns=['Freight PO', 'Car Cost PO','Transit Days','Lbs', 'Throughput Invoice', 'Shop Order #' ], axis=1, inplace=True)
#df1 = df1.replace('\n','', regex=True)
df1.rename(columns={"Transport\nTask": 'Transport Task', 'Rail Car #': 'Rail Car Number' }, inplace=True)
df1.dropna(subset=['Arrival Date'], how='any', inplace=True)
df1.fillna(0, inplace=True)
#print(df1.columns)

#print(df1.loc[df1['Transport Task'] == 20303 ])
selected_rows = df1.loc[df1['Arrival Date'] == '01-Jan-23']

# Print the selected rows
#print(selected_rows)
try:
    split_index = (df1['Demand Site'] == 'PENDING').idxmax()
    if split_index != 0:
    # Split the DataFrame into two parts
        df2 = df1.iloc[:split_index]
        df3 = df1.iloc[split_index:]
        df3 = df3.iloc[1:]
        #print(split_index)
        #print(df2.head(20))
        #print(df3.head(20))
        lm(LOG, "Found Pending cell and split the sheet")
        
    else:
        df2 = df1

except Exception as e:
    msteam(site, "CANNOT FIND PENDING AND LOGIC DOES NOT FIX, SEE do_data_category.py Error: " + str(e),"Trent Radford")
    lm(LOG, "Error: " + str(e))
    #print(e)    
# Define a function to convert the date format


def convert_date_format(date_input):
    current_year = datetime.datetime.now().year
    
    try:
        date_obj = pd.to_datetime(date_input, format='%d-%b')
    except ValueError:
        date_obj = pd.to_datetime(date_input)
    
    # Check if the month is Oct, Nov, or Dec and adjust the year
    if date_obj.month in [10, 11, 12]:
        date_obj = date_obj.replace(year=current_year - 1)
    else:
        date_obj = date_obj.replace(year=current_year)
    
    return date_obj.strftime('%d-%b-%Y')

# Apply the conversion function to the 'Date' column in the DataFrame
df2['Ship Date'] = df2['Ship Date'].apply(convert_date_format)
df2['Arrival Date'] = df2['Arrival Date'].apply(convert_date_format)
# Convert the 'Ship Date' and 'Arrival Date' columns to a format Oracle can understand
def format_date(date_input):
    if isinstance(date_input, pd.Timestamp):
        return date_input.strftime('%d-%b-%Y')
    else:
        return date_input

# Apply the custom formatting function to the 'Ship Date' and 'Arrival Date' columns
df2['Ship Date'] = df2['Ship Date'].apply(format_date)
df2['Arrival Date'] = df2['Arrival Date'].apply(format_date)
selected_rows = df2.loc[df1['Arrival Date'] == '01-Jan-23']

# Print the selected rows
#print(selected_rows)
df2.dropna(subset=['Transport Task'], how='any', inplace=True)
df2.drop(df2[df2['Transport Task'] == 0].index, inplace=True)
#print(df2.head(20))


#df3.columns =df2.columns
# Display the two DataFrames
pd.set_option('display.max_rows', None)
pd.set_option('display.min_rows', None)





# ######## Establish a connection to the database

lib_dir = r"path"
os.chdir(lib_dir)
#try:
#engine = create_engine('oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}')


conn = cx_Oracle.connect("")

# Create a cursor to execute SQL statements
cursor = conn.cursor()

######### DROP TABLE BEFORE INSERT
drop_table_sql = '''
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DO_TRANS_TASK_TAB';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
'''

# # Execute the PL/SQL block
cursor.execute(drop_table_sql)

# # Commit the changes to the database
conn.commit()


# # drp = '''DROP TABLE DO_TRANS_TASK_TAB'''
# # cursor.execute(drp)
# # conn.commit()

crt = '''CREATE TABLE DO_TRANS_TASK_TAB (
    Demand_Site          VARCHAR2(255),
    Origin_Id            VARCHAR2(255),
    Transport_Task       NUMBER,
    Destination             VARCHAR2(255),
    Product              VARCHAR2(255),
    Tons                 NUMBER,
    Rail_Car_Number      VARCHAR2(255),
    Ship_Date            DATE,
    Arrival_Date         DATE
    
)'''

cursor.execute(crt)
conn.commit()
######## Update the Transport task table with new data

insert_sql_task = '''
    INSERT INTO DO_TRANS_TASK_TAB (
        Demand_Site, 
        Origin_Id, 
        Transport_Task, 
        Destination,
        Product, 
        Tons, 
        Rail_Car_Number, 
        Ship_Date, 
        Arrival_Date
        
    ) VALUES (
        :Demand_Site, 
        :Origin_Id, 
        :Transport_Task,
        :Destination, 
        :Product, 
        :Tons, 
        :Rail_Car_Number, 
        :Ship_Date, 
        :Arrival_Date
        
    )
'''

# Bind the values to the parameters in the update statement
for index, row in df2.iterrows():
    cursor.execute(insert_sql_task, {
        'Demand_Site': row['Demand Site'],
        'Origin_Id': row['Origin Id'],
        'Transport_Task': row['Transport Task'],
        'Destination': row['Destination'],
        'Product': row['Product'],
        'Tons': row['Tons'],
        'Rail_Car_Number': row['Rail Car Number'],
        'Ship_Date': row['Ship Date'],
        'Arrival_Date': row['Arrival Date']
        

})

# # Commit the changes to the database
conn.commit()



#############THIS IS WHERE THE DO DATA DICTIONARY STARTS ################
###### Step 4 Read the Tank fleet Rail Shipment data into a dataframe

df = pd.read_excel(r"path"
#print(df.head(20))
df = df.astype('str')

################################### 
drop_table_sql_2 = '''
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DO_RAIL_DATA_TAB_2';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
'''

# # Execute the PL/SQL block
cursor.execute(drop_table_sql_2)

# # Commit the changes to the database
conn.commit()


# # drp = '''DROP TABLE DO_TRANS_TASK_TAB'''
# # cursor.execute(drp)
# # conn.commit()

crt_2 = '''CREATE TABLE DO_RAIL_DATA_TAB_2 (
  name_reference VARCHAR2(100),
    dmc_car VARCHAR2(50)
)'''

cursor.execute(crt_2)
conn.commit()

insert_sql2 = '''
        INSERT INTO DO_RAIL_DATA_TAB_2 (name_reference, dmc_car
        
)
        VALUES (:name_reference, :dmc_car


       
        )
    '''

    ##Bind the values to the parameters in the insert statement
for index, row in df.iterrows():
    cursor.execute(insert_sql2, {
        'name_reference': row['NAME REFERENCE'],
        'dmc_car': row['DMC Cars = C DMC']
 

     
    })
    # the connection is not autocommitted by default, so we must commit to save our changes


## the connection is not autocommitted by default, so we must commit to save our changes
conn.commit()
#########################################

drop_table_sql_3 = '''
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DO_RAIL_DATA_TAB_3';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
'''

# # Execute the PL/SQL block
cursor.execute(drop_table_sql_3)

# # Commit the changes to the database
conn.commit()


# # drp = '''DROP TABLE DO_TRANS_TASK_TAB'''
# # cursor.execute(drp)
# # conn.commit()

crt_3 = '''CREATE TABLE DO_RAIL_DATA_TAB_3 (
 "ORIGIN_ID" VARCHAR2(20 BYTE), 
"ORIGIN" VARCHAR2(50 BYTE)
)'''

cursor.execute(crt_3)
conn.commit()

insert_sql3 = '''
        INSERT INTO DO_RAIL_DATA_TAB_3 (ORIGIN_ID, ORIGIN
        
)
        VALUES (:origin_id, :origin


       
        )
    '''

    ##Bind the values to the parameters in the insert statement
for index, row in df.iterrows():
    cursor.execute(insert_sql3, {
        'origin_id': row['ORIGIN ID'],
        'origin': row['ORIGIN']
 

     
    })
    # the connection is not autocommitted by default, so we must commit to save our changes


## the connection is not autocommitted by default, so we must commit to save our changes
conn.commit()


#####################################
drop_table_sql_4 = '''
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DO_RAIL_DATA_TAB_4';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
'''

# # Execute the PL/SQL block
cursor.execute(drop_table_sql_4)

# # Commit the changes to the database
conn.commit()

crt4 = '''CREATE TABLE "IFSAPP"."DO_RAIL_DATA_TAB_4" (
"PART_DESCRIPTION" VARCHAR2(100 BYTE), 
"PRODUCT_CATEGORY" VARCHAR2(50 BYTE)
) '''
   
cursor.execute(crt4)
conn.commit()

insert_sql4 = '''
        INSERT INTO DO_RAIL_DATA_TAB_4 (PART_DESCRIPTION,
PRODUCT_CATEGORY
        
)
        VALUES (:PART_DESCRIPTION,
:PRODUCT_CATEGORY


       
        )
    '''

    ##Bind the values to the parameters in the insert statement
for index, row in df.iterrows():
    cursor.execute(insert_sql4, {
        'PART_DESCRIPTION': row['PART DESCRIPTION'],
        'PRODUCT_CATEGORY': row['PRODUCT CATEGORY']
 

     
    })
    # the connection is not autocommitted by default, so we must commit to save our changes


## the connection is not autocommitted by default, so we must commit to save our changes
conn.commit()

################################
drop_table_sql_ = '''
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DO_RAIL_DATA_TAB';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
'''

# # Execute the PL/SQL block
cursor.execute(drop_table_sql_)

# # Commit the changes to the database
conn.commit()
####### Create the table and insert the data into the table

crt_ = '''CREATE TABLE DO_RAIL_DATA_TAB
   (	"DESTINATION_NAME" VARCHAR2(100 BYTE), 
	"REGION" VARCHAR2(50 BYTE), 
	"TYPE" VARCHAR2(10 BYTE), 
	"SCORECARD_CATEGORY" VARCHAR2(50 BYTE)
   ) '''
   
cursor.execute(crt_)
conn.commit()

insert_sql_ = '''
        INSERT INTO DO_RAIL_DATA_TAB (DESTINATION_NAME,
REGION,
TYPE,
SCORECARD_CATEGORY

        
)
        VALUES (:DESTINATION_NAME,
:REGION,
:TYPE,
:SCORECARD_CATEGORY



       
        )
    '''

    ##Bind the values to the parameters in the insert statement
for index, row in df.iterrows():
    cursor.execute(insert_sql_, {
        'DESTINATION_NAME': row['DESTINATION NAME'],
        'REGION': row['REGION'],
        'TYPE': row['TYPE'],
        'SCORECARD_CATEGORY': row['SCORECARD CATEGORY']



     
    })


conn.commit()
conn.close()

