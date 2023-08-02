from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine, VARCHAR
import pyodbc

# Define the MSSQL connection string
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
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + con_string)
if engine:
    print("Connection established")
else:
    print("Connection failed")

# Function to save the DataFrame to the SQL Server
def save_to_sql(dataframe, table_name):
    # Create a cursor object to execute SQL queries
    cursor = engine.cursor()

# Create the table in the database (if not exists)
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (name VARCHAR(255), Channel_URL VARCHAR(255))")
    # Convert the DataFrame to a list of tuples

    rows = [tuple(row) for row in dataframe.to_numpy()]

    # Insert the rows into the table
    cursor.executemany(f"INSERT INTO {table_name} (event_type, data) VALUES (?, ?)", rows)

    # Commit the transaction
    engine.commit()

    # Close the cursor
    cursor.close()

app = FastAPI()

class WebhookPayload(BaseModel):
    name: str
    Channel_URL: str


@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    # Access the payload fields
    name = payload.name
    Channel_URL = payload.Channel_URL

    # Create a DataFrame from the payload data
    df = pd.DataFrame([(name, Channel_URL)], columns=['name', 'Channel_URL'])

    # Save the DataFrame to the SQL Server
    df.to_sql('webhook', con=engine, if_exists='append', index=False)

    return {"message": "Webhook data saved successfully to SQL Server"}


  # run this in the terminal uvicorn test:app --reload



  