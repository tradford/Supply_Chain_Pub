
import pandas as pd
import oracle_connect2 as oracle_connect
import cx_Oracle
import os
# Load data from a specific sheet in an Excel file into a pandas DataFrame
excel_path = r'path"
sheet_name = "Sheet1"
df = pd.read_excel(excel_path, sheet_name=sheet_name)
# Establish a connection to the database and create a cursor
lib_dir = r"path"
os.chdir(lib_dir)
orc = oracle_connect.Oracle()
orc.connect_node()
conn = cx_Oracle.connect("", "", "")
cursor = conn.cursor()
# Processing each row in the DataFrame
for index, row in df.iterrows():
    customer_no = str(row['CUST ID'])
    market2_update = row['Market2 Update'] if pd.notna(row['Market2 Update']) else None
    segment = row['Segment Update'] if pd.notna(row['Segment Update']) else None
    # Trying to find a matching rowkey in CUST_ORD_CUSTOMER_ADDRESS_CFT
    query = """
    SELECT A.rowkey
    FROM CUST_ORD_CUSTOMER_ADDRESS_TAB A
    LEFT JOIN CUST_ORD_CUSTOMER_ADDRESS_CFT B ON A.rowkey = B.rowkey
    WHERE A.customer_no = :customer_no
    """
    cursor.execute(query, customer_no=customer_no)
    result = cursor.fetchone()
    
    if result:
        rowkey = result[0]
        
        check_query = """
        SELECT 1
        FROM CUST_ORD_CUSTOMER_ADDRESS_CFT
        WHERE ROWKEY = :rowkey
        """
        cursor.execute(check_query, rowkey=rowkey)
        exists = cursor.fetchone()
        
        if exists:  # Update if rowkey exists
            update_query = """
             UPDATE CUST_ORD_CUSTOMER_ADDRESS_CFT
            SET CF$_MARKET2 = COALESCE(:market2_update, CF$_MARKET2),
                CF$_SEGMENT = COALESCE(:segment, CF$_SEGMENT)
            WHERE ROWKEY = :rowkey
            """
            params = {'rowkey': rowkey, 'market2_update': market2_update, 'segment': segment}
            cursor.execute(update_query, params)
        
        else:  # If rowkey doesn’t exist in CFT, get a new rowkey where addr_no = '1'
            get_rowkey_query = """
            SELECT rowkey
            FROM CUST_ORD_CUSTOMER_ADDRESS_TAB
            WHERE addr_no = '1' AND customer_no = :customer_no
            """
            cursor.execute(get_rowkey_query, customer_no=customer_no)
            new_rowkey_result = cursor.fetchone()
            
            if new_rowkey_result:  # If a new rowkey is found, insert a new record
                new_rowkey = new_rowkey_result[0]
                insert_query = """
                INSERT INTO CUST_ORD_CUSTOMER_ADDRESS_CFT (ROWKEY, CF$_MARKET2, CF$_SEGMENT)
                VALUES (:new_rowkey, :market2_update, :segment)
                """
                params = {'new_rowkey': new_rowkey, 'market2_update': market2_update, 'segment': segment}
                cursor.execute(insert_query, params)
            
        # Commit the transaction
        conn.commit()
# Don’t forget to close the cursor and connection when you're done
cursor.close()
conn.close()
