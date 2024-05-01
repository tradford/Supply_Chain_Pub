import pandas as pd
from sqlalchemy import create_engine, text
import os
import oracle_connect2 as oracle_connect
import cx_Oracle

# Establish a connection to the database
lib_dir = r"path"
os.chdir(lib_dir)
orc = oracle_connect.Oracle()
orc.connect_node()
conn = cx_Oracle.connect("")

# Create a cursor to execute SQL statements
cursor = conn.cursor()



query = """
SELECT *
FROM CUST_ORD_CUSTOMER_ADDRESS_TAB A
JOIN CUST_ORD_CUSTOMER_ADDRESS_CFT B ON A.rowkey = B.rowkey
"""
cursor.execute(query)

data = cursor.fetchall()

# # Printing the data
# for row in data:
#     print(row)
##Step 2: Load excel file into a dataframe
excel_path = r'path"
sheet_name = "Sortable Copy"
df = pd.read_excel(excel_path, sheet_name=sheet_name)

# # Step 3: Update the database
for index, row in df.iterrows():
    if pd.notna(row['Market2 Update']):
        customer_no = str(row['CUST ID'])
        market2_update = str(row['Market2 Update'])
        
        update_query = """
        UPDATE CUST_ORD_CUSTOMER_ADDRESS_CFT
        SET CF$_MARKET2 = :market2_update
        WHERE rowkey IN (
            SELECT B.rowkey
            FROM CUST_ORD_CUSTOMER_ADDRESS_TAB A
            JOIN CUST_ORD_CUSTOMER_ADDRESS_CFT B ON A.rowkey = B.rowkey
            WHERE A.CUSTOMER_NO = :customer_no
        )
        """
        
        params = {'market2_update': market2_update, 'customer_no': customer_no}
        cursor.execute(update_query, params)
        conn.commit()
