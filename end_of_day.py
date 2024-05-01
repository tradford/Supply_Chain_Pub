import pandas as pd
import os
import oracle_connect2 as oracle_connect
import cx_Oracle


df = pd.read_csv(r'path"
print(df.columns.to_list())

df['Date'] = pd.to_datetime(df['Date'])

# Extract the DO# from the ' Print_DO#' column
print(df[' Print_DO#'].dtype)
df['Print_DO#'] = df[' Print_DO#'].str[7:-1]
df['Driver'] = df[' Driver'].str[2:-1]
df['Part_ Des'] = df[' Part_ Des'].str[2:-1]
df['Del_Qty'] = df[' Del_Qty']

# Display the first few rows of the dataframe
print(df.head())
# Create a new dataframe that contains only the rows where 'Print_DO#' starts with '2' and has 5 digits
df_task = df[(df['Print_DO#'].str.startswith('2')) & (df['Print_DO#'].str.len() == 5)]

# Display the first few rows of the filtered dataframe
print(df_task.head())

df['Print_DO#'] = pd.to_numeric(df['Print_DO#'])
try:
    dfdup = df[df.duplicated(['Print_DO#'])]
    if dfdup is not None:
        print("notifying")
        #msteams message to carrie, chris and amanda
except Exception as e:
    print("Error: ", e)
print(df.dtypes)


# Establish a connection to the database
lib_dir = r"path"
os.chdir(lib_dir)
try:
    conn = cx_Oracle.connect("")

    # Create a cursor to execute SQL statements
    cursor = conn.cursor()

    # Create the table
    update_sql = '''UPDATE c_delivery_order_tab SET 
                ACTUAL_PICKUP_DATE_TIME = :pickup_date_time, 
                ACTUAL_DEL_DATE_TIME = :delivery_date_time, 
                DRIVER = :driver,
                RECEIPT_REFERENCE = :del_ord_no, 
                QTY_RECEIVED = :qty_received
                WHERE DEL_ORD_NO = :del_ord_no'''

# Bind the values to the parameters in the update statement
    for index, row in df.iterrows():
        cursor.execute(update_sql, {
            'pickup_date_time': row['Date'],
            'delivery_date_time': row['Date'],
            'del_ord_no': row['Print_DO#'],
            'driver': row['Driver'],
            'qty_received': row['Del_Qty']
    })

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()
except cx_Oracle.DatabaseError as e:
    print(e)
            # Log error as appropriate
    raise

