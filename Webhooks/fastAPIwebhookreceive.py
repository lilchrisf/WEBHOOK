from fastapi import FastAPI, Request, HTTPException
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

app = FastAPI()
 # Create an empty DataFrame
df = pd.DataFrame()  

# Define the MSSQL connection string
server_name = "AGLJ-LAP-032"
database_name = "master"
username = "sqladmin"
password = "Admin@12345"
# Set up the database connection
con_string = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server_name};"
    f"Database={database_name};"
    f"UID={username};"
    f"PWD={password};"
)

conn = pyodbc.connect(con_string)
engine = create_engine('mssql+pyodbc://', creator=lambda: conn)

@app.post('/webhook')
async def webhook(request: Request):
# Declare df as a global variable
    global df 
    try:
# Get the JSON data from the request
        data = await request.json() 
        if isinstance(data, dict):
# If the JSON data is a dictionary, convert it to a DataFrame
            df_new = pd.DataFrame([data])
        elif isinstance(data, list):
# If the JSON data is a list, assume it contains dictionaries and convert it to a DataFrame
            df_new = pd.DataFrame(data)
        else:
            raise ValueError("Invalid JSON data format")
        
# Append the new DataFrame to the existing DataFrame
        df = pd.concat([df, df_new], ignore_index=True)
        print(df)  # Print the updated DataFrame

# Save DataFrame to SQL database
        df.to_sql('webhook_data', engine, if_exists='replace', index=False)

        return {'message': 'success'}
    except ValueError as e:
        print(f"Failed to process webhook data: {e}")
        raise HTTPException(status_code=400, detail="Failed to process webhook data")
    
# run this in the terminal uvicorn fastAPIwebhookreceive:app --reload

