import pandas as pd
import pyodbc
import pytz
from sqlalchemy import create_engine
import sqlalchemy

# Read the Excel file and select specific columns
selected_columns = [0, 1, 2, 3, 6, 7, 13, 14]
df = pd.read_csv('C:\\xampp\\htdocs\\python\\Binary_search\\Webhooks\\excel.csv', usecols=selected_columns)

# Apply filter to include only complete or active rows
df = df[(df['Survey completion'] == 'FINISHED') | (df['Survey completion'] == 'ACTIVE')]

# Convert GMT to Eastern Time Zone
gmt_tz = pytz.timezone('GMT')
eastern_tz = pytz.timezone('US/Eastern')

df['Start time(GMT)'] = pd.to_datetime(df['Start time(GMT)'], format='%I:%M:%S %p')
df['Start time(GMT)'] = df['Start time(GMT)'].dt.tz_localize(gmt_tz).dt.tz_convert(eastern_tz).dt.strftime('%I:%M %p')

df['Transaction timestamp'] = pd.to_datetime(df['Transaction timestamp'], format='%A, %d %B %Y')
df['Transaction timestamp'] = df['Transaction timestamp'].dt.tz_localize(gmt_tz).dt.tz_convert(eastern_tz).dt.strftime('%A, %d %B %Y')

# Create a connection string to the SQL Server database
connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=AGLJ-LAP-032\SQLEXPRESS;DATABASE=master;TRUSTED_CONNECTION=yes;"
server_name = "AGLJ-LAP-032"
database_name = "master"
username = "sqladmin"
password = "Admin@12345"
# set up the database connection string
con_string = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server_name};"
    f"Database={database_name};"
    f"UID={username};"
    f"PWD={password}"
)

# Create a connection to the SQL Server database
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={con_string}")

# Define the data types for each column
dtypes = {
    'User ID': sqlalchemy.types.Integer,
    'Transaction ID': sqlalchemy.types.Integer,
    'Transaction timestamp': sqlalchemy.types.String,
    'Survey completion': sqlalchemy.types.String,
    'Gender': sqlalchemy.types.String,
    'Phone number': sqlalchemy.types.String,
    'location': sqlalchemy.types.Integer,
    'Start time(GMT)': sqlalchemy.types.String,
}

# Save the DataFrame to the database
df.to_sql('excel', engine, if_exists='append', index=False, dtype=dtypes, method='multi', chunksize=1000)

# Close the database connection
engine.dispose()


# run the following code in the terminal= python excel.py