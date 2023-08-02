from flask import Flask, request, abort
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import certifi
import os
import requests

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

app = Flask(__name__)

df = pd.DataFrame()  # Create an empty DataFrame
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
# connect the variable to the server
conn = pyodbc.connect(con_string)

if conn:
    print("Connection established.")
else:
    print("Connection failed.")

def get_connection():
    return pyodbc.connect(con_string)

@app.route('/webhook', methods=['POST'])
def webhook():
    global df  # Declare df as a global variable

    if request.method == 'POST':
        try:
            data = request.json  # Get the JSON data from the request

            if isinstance(data, dict):
                # If the JSON data is a dictionary, convert it to a DataFrame
                df_new = pd.DataFrame([data])
            elif isinstance(data, list):
                # If the JSON data is a list, assume it contains dictionaries and convert it to a DataFrame
                df_new = pd.DataFrame(data)
            else:
                raise ValueError("Invalid JSON data format")

            df = pd.concat([df, df_new], ignore_index=True)  # Concatenate the new DataFrame to the existing DataFrame
            print(df)  # Print the updated DataFrame

            # Save DataFrame to SQL database
            engine = create_engine('mssql+pyodbc://', creator=get_connection)
            df.to_sql('webhook_data', engine, if_exists='append', index=False)  # Replace 'webhook_data' with your desired table name

        
            webhook_url = "http://api.appsdev.jncb.com:5000/api/ajua-webhooks/webhook"

            # Send the webhook to the modified URL
            response = requests.post(webhook_url, json=data)

            if response.status_code == 200:
                print("Webhook sent successfully.")
            else:
                print("Failed to send webhook. Status Code:", response.status_code)

            return 'success', 200
        except ValueError as e:
            print(f"Failed to process webhook data: {e}")
            abort(400)
    else:
        abort(400)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
