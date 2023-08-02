from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

app = FastAPI()

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
# Create a SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={con_string}")


class WebhookData(BaseModel):
    name: str
    Channel_URL: str

# Create an empty DataFrame
df = pd.DataFrame()

def insert_data_into_sql():
    global df
    # Create a table if it doesn't exist or append to the existing table
    df.to_sql('webhook', con=engine, if_exists='append', index=False)
    # Clear the DataFrame after insertion
    df = pd.DataFrame()
    print("Data inserted into SQL table at", datetime.now())

@app.post('/webhook')
async def webhook(request: Request, data: WebhookData, background_tasks: BackgroundTasks):
    try:
        # Convert the validated data to a DataFrame
        df_new = pd.DataFrame([data.dict()])

        # Append the new DataFrame to the existing DataFrame
        global df
        df = pd.concat([df, df_new], ignore_index=True)

        # Schedule data insertion task every 6 hours
        schedule = timedelta(hours=6)
        # schedule = timedelta(seconds=5)
        background_tasks.add_task(insert_data_into_sql)
        
        return {'message': 'success'}
    except ValueError as e:
        print(f"Failed to process webhook data: {e}")
        raise HTTPException(status_code=422, detail="Invalid webhook data")


# run this in the terminal: uvicorn time_1:app --reload
# pip install fastapi
# pip install uvicorn
# pip install pydantic
# pip install pandas
# pip install sqlalchemy
# pip install asyncio

