from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import asyncio

app = FastAPI()

# Define the MSSQL connection string
server_name = "AGLJ-LAP-032"
database_name = "WEBHOOK"
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

# Create a SQLAlchemy engine and session
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={con_string}")
Session = sessionmaker(bind=engine)
session = Session()

class WebhookData(BaseModel):
    name: str
    Channel_URL: str

# Define the SQLAlchemy model for the 'webhook' table
Base = declarative_base()

class WebhookEntry(Base):
    __tablename__ = 'webhook'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(250))  
    Channel_URL = Column(String(250)) 

# Create the table if it doesn't exist
Base.metadata.create_all(engine) 

# Create an empty list to store the webhook entries
webhook_entries = []
last_insert_time = datetime.now()

@app.post('/webhook')
async def webhook(request: Request, data: WebhookData, background_tasks: BackgroundTasks):
    try:
        # Create a new webhook entry
        entry = WebhookEntry(name=data.name, Channel_URL=data.Channel_URL)

        # Append the new entry to the list
        webhook_entries.append(entry)

        return {'message': 'success'}
    except ValueError as e:
        print(f"Failed to process webhook data: {e}")
        raise HTTPException(status_code=422, detail="Invalid webhook data")

async def bulk_insert_scheduler():
    while True:
        if webhook_entries:
            bulk_insert()
        await asyncio.sleep(15)  # Run bulk insertion every 5 seconds

def bulk_insert():
    try:
        global webhook_entries

        # Copy the webhook entries list and clear it
        entries = webhook_entries.copy()
        webhook_entries.clear()

        # Add the entries to the session and commit the changes
        session.add_all(entries)
        session.commit()

        print("Bulk insertion performed at", datetime.now())
    except Exception as e:
        print(f"Failed to perform bulk insertion: {e}")


# Start the scheduler task
asyncio.create_task(bulk_insert_scheduler())




# run this in the terminal: uvicorn bulktime:app --reload
# pip install fastapi
# pip install uvicorn
# pip install pydantic
# pip install pandas
# pip install sqlalchemy
# pip install asyncio

