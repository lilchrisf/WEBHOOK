import pandas as pd
from bs4 import BeautifulSoup
import requests
import pyodbc

webhook_site_url = 'https://customer.guru/net-promoter-score/top-brands'

# Send a GET request to retrieve the data
response = requests.get(webhook_site_url)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find_all('table')
rows = soup.find_all('tr')
   
data = []

for row in rows:
    cells = row.find_all('td')
    row_data = [cell.text.strip() for cell in cells]
    data.append(row_data)

# Define column names
column_names = ['ID', 'IMG', 'COMPANY', 'INDUSTRY', 'WEBSITE', 'NPS_SCORE', 'PROMOTER_SCORE']

df = pd.DataFrame(data, columns=column_names)

# Connect to the SQL Server
con_string = "DRIVER={SQL Server};SERVER=AGLJ-LAP-032\SQLEXPRESS;DATABASE=Exam;TRUSTED_CONNECTION=yes;"
conn = pyodbc.connect(con_string)

# Create the SQL table
def create_table(conn):
    DROP_Name = 'Truuu'
    cursor = conn.cursor()
    Drop_table = f""" DROP TABLE IF EXISTS {DROP_Name} """
    cursor.execute(Drop_table)

create_table(conn)
conn.commit()

# Create the SQL table
def create_table(conn):
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE Truuu 
    (
        ID INT NULL, 
        IMG VARCHAR(20) NULL, 
        COMPANY VARCHAR(200) NULL, 
        INDUSTRY VARCHAR(200) NULL, 
        WEBSITE VARCHAR(200) NULL,
        NPS_SCORE INT NULL,
        PROMOTER_SCORE VARCHAR(200) NULL
    )
    """
    cursor.execute(create_table_query)

create_table(conn)
conn.commit()

# Insert data into the SQL table
cursor = conn.cursor()
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO Truuu (ID, IMG, COMPANY, INDUSTRY, WEBSITE, NPS_SCORE, PROMOTER_SCORE) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    values = tuple(row)
    cursor.execute(insert_query, values)
if (insert_query, values):
    print('created succesfully')
else:
    print('Table did not create')
conn.commit()

# run the following code in the terminal (python webhookdb.py)






