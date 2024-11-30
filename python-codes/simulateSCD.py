import pandas as pd
import pyodbc
from datetime import datetime

# Database connection setup
connection = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ZYGUEL;"
    "Database=VehicleFuelEfficiencyDB;"
    "Trusted_Connection=yes;"
)

cursor = connection.cursor()

# Load datasets
vehicles_df = pd.read_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles_converted.csv')
emissions_df = pd.read_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_emissions.csv')
gasoline_df = pd.read_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\gasoline_unified.csv')
diesel_df = pd.read_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\diesel_unified.csv')

# Simulate Type II changes for DimVehicle
def simulate_vehicle_type2_changes(df):
    changes = []
    for index, row in df.iterrows():
        if row['id'] == 1:  # Example: modifying record for id = 1
            # Old record
            changes.append({
                'id': row['id'], 'make': row['make'], 'model': row['model'],
                'eng_dscr': row['eng_dscr'], 'trany': row['trany'],
                'EffectiveDate': '2013-01-01', 'EndDate': '2015-01-01', 'IsCurrent': 0
            })
            # New record
            changes.append({
                'id': row['id'], 'make': row['make'], 'model': row['model'],
                'eng_dscr': '(Updated Description)', 'trany': 'Automatic 6-spd',
                'EffectiveDate': '2015-01-01', 'EndDate': None, 'IsCurrent': 1
            })
        else:
            # Existing record remains unchanged
            changes.append({
                'id': row['id'], 'make': row['make'], 'model': row['model'],
                'eng_dscr': row['eng_dscr'], 'trany': row['trany'],
                'EffectiveDate': '2013-01-01', 'EndDate': None, 'IsCurrent': 1
            })
    return pd.DataFrame(changes)

# Update DimVehicle with Type II changes
dim_vehicle = simulate_vehicle_type2_changes(vehicles_df)

# Insert into SQL
for index, row in dim_vehicle.iterrows():
    cursor.execute("""
        INSERT INTO DimVehicle (id, make, model, eng_dscr, trany, EffectiveDate, EndDate, IsCurrent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, row['id'], row['make'], row['model'], row['eng_dscr'], row['trany'],
       row['EffectiveDate'], row['EndDate'], row['IsCurrent'])

connection.commit()

# Simulate Type II changes for DimFuel
# (Example: Update price records for a specific date range)

def simulate_fuel_type2_changes(df):
    changes = []
    for index, row in df.iterrows():
        if row['Day'] == '2014-01-01':  # Example of price update
            # Old price record
            changes.append({
                'Day': row['Day'], 'Price': row['Price'],
                'EffectiveDate': '2014-01-01', 'EndDate': '2015-01-01', 'IsCurrent': 0
            })
            # New price record
            changes.append({
                'Day': '2015-01-01', 'Price': 4.000,  # Example updated price
                'EffectiveDate': '2015-01-01', 'EndDate': None, 'IsCurrent': 1
            })
        else:
            changes.append({
                'Day': row['Day'], 'Price': row['Price'],
                'EffectiveDate': row['Day'], 'EndDate': None, 'IsCurrent': 1
            })
    return pd.DataFrame(changes)

# Apply changes to fuel prices
dim_fuel = simulate_fuel_type2_changes(diesel_df)

# Insert into SQL
for index, row in dim_fuel.iterrows():
    cursor.execute("""
        INSERT INTO DimFuel (Day, Price, EffectiveDate, EndDate, IsCurrent)
        VALUES (?, ?, ?, ?, ?)
    """, row['Day'], row['Price'], row['EffectiveDate'], row['EndDate'], row['IsCurrent'])

connection.commit()

cursor.close()
connection.close()
