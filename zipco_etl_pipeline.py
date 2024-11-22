import requests
import json
import pandas as pd
import csv
import psycopg2
import datetime as dt
from datetime import datetime, timedelta

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "64114f9223mshf6ee7f915c727eep1a5d9djsnef3a9270cef8",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

data = response.json()

# save data to json file
filename = 'PropertyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)

# Read into a DataFrame
propertyrecords_df = pd.read_json('PropertyRecords.json')

# Transformation layer
# 1st step - convert dictionary column to string
propertyrecords_df['features'] = propertyrecords_df['features'].apply(json.dumps)
propertyrecords_df['owner'] = propertyrecords_df['owner'].apply(json.dumps)
propertyrecords_df['ownerOccupied'] = propertyrecords_df['ownerOccupied'].apply(json.dumps)

# 2nd step - replace NaN values with appropriate defaults or remove  row/columns as necessary
propertyrecords_df.fillna({
    'assessorID': 'Unknown',
    'legalDescription': 'Not available',
    'squareFootage': 0,
    'subdivision': 'Not available',
    'yearBuilt': 0,
    'bathrooms': 0,
    'lotSize': 0,
    'propertyType': 'Unknown',
    'lastSalePrice': 0,
    'lastSaleDate': 'Not available',
    'features': 'None',
    'taxAssessment': 'Not available',
    'owner': 'Unknown',
    'propertyTaxes': 'Not available',
    'bedrooms': 0,
    'ownerOccupied': 0,
    'zoning': 'Unknown',
    'addressLine2': 'Not available',
    'formattedAddres': 'Not available',
    'county': 'Not available',
}, inplace = True)

# Convert to datetime with coercion of invalid entries
propertyrecords_df['lastSaleDate'] = pd.to_datetime(
    propertyrecords_df['lastSaleDate'], 
    errors='coerce'  # Converts invalid formats to NaT
)

location = propertyrecords_df[['formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude', 'zoning']].copy().drop_duplicates().reset_index(drop=True)
location['location_id'] =range(1, len(location) + 1)
location = location[['location_id', 'formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude', 'zoning']]

property = propertyrecords_df[['propertyType', 'bedrooms', 'bathrooms', 'squareFootage', 'yearBuilt', 'legalDescription', 'lastSaleDate', 'ownerOccupied', 'lotSize']].copy().drop_duplicates().reset_index(drop=True)
property['property_id'] =range(1, len(property) + 1)
property = property[['property_id', 'propertyType', 'bedrooms', 'bathrooms', 'squareFootage', 'yearBuilt', 'legalDescription', 'lastSaleDate', 'ownerOccupied', 'lotSize']]

sales = propertyrecords_df[['lastSaleDate', 'lastSalePrice']].copy().drop_duplicates().reset_index(drop=True)
sales['sales_id'] =range(1, len(sales) + 1)
sales = sales[['sales_id', 'lastSaleDate', 'lastSalePrice']]

# merge operation to create the propertyrecords_df
propertyrecords_df = propertyrecords_df.merge(location, on=['formattedAddress', 'city', 'state', 'zipCode', 'county', 'subdivision', 'longitude', 'latitude', 'zoning'], how ='left') \
                            .merge(property, on=['propertyType', 'bedrooms', 'bathrooms', 'squareFootage', 'yearBuilt', 'legalDescription', 'lastSaleDate', 'ownerOccupied', 'lotSize'], how='left') \
                            .merge(sales, on=['lastSaleDate', 'lastSalePrice'], how='left') \
                            [['location_id', 'property_id', 'sales_id']]

# creating a date dimension table
start_date = datetime(2020, 1, 1)
current_date = datetime(2090, 12, 31)

# calculate the number of days between start date and current date
num_days = (current_date - start_date).days

# Generate a list of dates from start date to current date
date_list = [start_date + timedelta(days=x) for x in range(num_days + 1)]

#Ensure date_id matches the length of date_list
date = {'date_id': [x for x in range(1, len(date_list) + 1)], 'date': date_list}

# Create DataFrame
date_dim = pd.DataFrame(date)
date_dim['Year'] = date_dim['date'].dt.year
date_dim['Month'] = date_dim['date'].dt.month
date_dim['Day'] = date_dim['date'].dt.day
date_dim['date'] = pd.to_datetime(date_dim['date']).dt.date

# link Property table with date
property['lastSaleDate'] = pd.to_datetime(property['lastSaleDate']).dt.date
property = property.merge(date_dim, left_on='lastSaleDate', right_on='date', how='inner') \
                    .rename(columns={'date_id':'lastSaleDate_ID'}) \
                    .reset_index(drop=True) \
                    [['property_id', 'propertyType', 'bedrooms', 'bathrooms', 'squareFootage', 'yearBuilt', 'legalDescription', 'lastSaleDate_ID', 'ownerOccupied', 'lotSize']]

# link sales table with date
sales['lastSaleDate'] = pd.to_datetime(sales['lastSaleDate']).dt.date
sales = sales.merge(date_dim, left_on='lastSaleDate', right_on='date', how='inner') \
                    .rename(columns={'date_id':'lastSaleDate_ID'}) \
                    .reset_index(drop=True) \
                    [['sales_id', 'lastSaleDate_ID', 'lastSalePrice']]

# Save to directory
location.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\location.csv', index=False)
property.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\property.csv', index=False)
sales.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\sales.csv', index=False)

# Propety DWH Model
property_dim = property[['property_id', 'propertyType']].copy().drop_duplicates().reset_index(drop=True)
sales_dim = sales[['sales_id', 'lastSaleDate_ID', 'lastSalePrice']].copy().drop_duplicates().reset_index(drop=True)
location_dim = location[['location_id', 'formattedAddress', 'city', 'state']].copy().drop_duplicates().reset_index(drop=True)

property_fact_table = propertyrecords_df.merge(property, on='property_id', how='inner') \
                                        .merge(sales, on='sales_id', how='inner') \
                                        [['location_id', 'property_id', 'sales_id']]
    

# creating a date dimension table
start_date = datetime(2020, 1, 1)
current_date = datetime(2090, 12, 31)

# calculate the number of days between start date and current date
num_days = (current_date - start_date).days

# Generate a list of dates from start date to current date
date_list = [start_date + timedelta(days=x) for x in range(num_days + 1)]

#Ensure date_id matches the length of date_list
date = {'date_id': [x for x in range(1, len(date_list) + 1)], 'date': date_list}

# Create DataFrame
date_dim = pd.DataFrame(date)
date_dim['Year'] = date_dim['date'].dt.year
date_dim['Month'] = date_dim['date'].dt.month
date_dim['Day'] = date_dim['date'].dt.day
date_dim['date'] = pd.to_datetime(date_dim['date']).dt.date

# save to memory
property_dim.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\property_dim.csv', index=False)
sales_dim.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\sales_dim.csv', index=False)
location_dim.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\location_dim.csv', index=False)
property_fact_table.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\property_fact_table.csv', index=False)
date_dim.to_csv(r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\date_dim.csv', index=False)

# Loading Layer
# develop a function to connect to pgadmin

def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'zipco_agency',
        user = 'postgres',
        password = 'Favour@8282'
    )
    return connection

conn = get_db_connection()

#create schema and tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
    CREATE SCHEMA IF NOT EXISTS zipco;

    DROP TABLE IF EXISTS zipco.property CASCADE;
    DROP TABLE IF EXISTS zipco.location CASCADE;
    DROP TABLE IF EXISTS zipco.sales CASCADE;
    DROP TABLE IF EXISTS zipco.date_dim CASCADE;
    DROP TABLE IF EXISTS zipco.property_fact_table CASCADE;
    
    CREATE TABLE IF NOT EXISTS zipco.date_dim (
        date_id SERIAL PRIMARY KEY,
        date VARCHAR(10000),
        Year INTEGER,
        Month INTEGER,
        Day INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS zipco.property (
        property_id SERIAL PRIMARY KEY,
        propertyType VARCHAR(255),
        bedrooms FLOAT,
        bathrooms FLOAT,
        squareFootage FLOAT,
        yearBuilt FLOAT,
        legalDescription VARCHAR(255),
        lastSaleDate_ID INTEGER,
        ownerOccupied VARCHAR(50),
        lotSize FLOAT,
        FOREIGN KEY (lastSaleDate_ID) REFERENCES zipco.date_dim(date_id)
    );
    
    CREATE TABLE IF NOT EXISTS zipco.location (
        location_id SERIAL PRIMARY KEY,
        formattedAddress VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(100),
        zipcode INTEGER,
        county VARCHAR(100),
        subdivision VARCHAR(100),
        longitude FLOAT,
        latitude FLOAT,
        zoning VARCHAR(100)
    );
    
    
    CREATE TABLE IF NOT EXISTS zipco.sales (
        sales_id SERIAL PRIMARY KEY,
        lastsaleDate_ID INTEGER,
        lastsalePrice VARCHAR(255),
        FOREIGN KEY (lastSaleDate_ID) REFERENCES zipco.date_dim(date_id)
        
    );
    
    CREATE TABLE IF NOT EXISTS zipco.property_fact_table (
        location_id INTEGER,
        property_id INTEGER,
        sales_id INTEGER,
        FOREIGN KEY (location_id) REFERENCES zipco.location(location_id),
        FOREIGN KEY (property_id) REFERENCES zipco.property(property_id),
        FOREIGN KEY (sales_id) REFERENCES zipco.sales(sales_id)
    );
    
    '''
        
    cursor.execute(create_table_query)
    conn.commit() 
    cursor.close()
    conn.close()

    
create_tables()    

# create a function to load the csv data into the database

def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit() 
    cursor.close()
    conn.close()  
            

location_csv_path = r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\location.csv'
load_data_from_csv_to_table(location_csv_path, 'zipco.location')

date_dim_csv_path = r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\date_dim.csv'
load_data_from_csv_to_table(date_dim_csv_path, 'zipco.date_dim')

sales_csv_path = r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\sales.csv'
load_data_from_csv_to_table(sales_csv_path, 'zipco.sales')

property_csv_path = r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\cleandata\property.csv'
load_data_from_csv_to_table(property_csv_path, 'zipco.property')

property_fact_table_dim_csv_path = r'C:\Users\Acer\OneDrive\Desktop\zipco_agency\zipco\dataset\transaction_dwh\property_fact_table.csv'
load_data_from_csv_to_table(property_fact_table_dim_csv_path, 'zipco.property_fact_table')

print('All data has been loaded successfully into the respective schema and tables')