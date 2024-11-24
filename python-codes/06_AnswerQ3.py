import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Database connection details
server = 'ZYGUEL'  # Your server name
database = 'VehicleFuelEfficiencyDB'  # Your database name

# Create a connection string for SQLAlchemy
connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'

# Establish a connection using SQLAlchemy
engine = create_engine(connection_string)

# Query to get the required data from the fact and dimension tables
query = '''
    SELECT 
        fe.average_mpg,
        fe.co2_emissions,
        e.fuelType,
        y.year
    FROM fact_vehicle_efficiency fe
    JOIN dim_engine e ON fe.engine_id = e.engine_id
    JOIN dim_year y ON fe.year_id = y.year_id
    WHERE y.year BETWEEN 2014 AND 2024
'''

# Execute the query and load the data into a DataFrame
df = pd.read_sql_query(query, engine)

# Filter data for EVs and HEVs
ev_hev_df = df[df['fuelType'].isin(['Electricity', 'Hybrid'])]

# Calculate the average MPG and CO2 emissions for EVs and HEVs over the years
average_mpg_ev_hev = ev_hev_df.groupby(['year', 'fuelType'])['average_mpg'].mean().reset_index()
average_co2_ev_hev = ev_hev_df.groupby(['year', 'fuelType'])['co2_emissions'].mean().reset_index()

# Plot average MPG over the years for EVs and HEVs
plt.figure(figsize=(14, 8))
for fuel_type in average_mpg_ev_hev['fuelType'].unique():
    subset = average_mpg_ev_hev[average_mpg_ev_hev['fuelType'] == fuel_type]
    plt.plot(subset['year'], subset['average_mpg'], marker='o', label=fuel_type)
plt.title('Average Fuel Efficiency (MPG) Change from 2014 to 2024 by Fuel Type')
plt.xlabel('Year')
plt.ylabel('Average MPG')
plt.legend(title='Fuel Type', loc='upper left', bbox_to_anchor=(1, 1))
plt.grid(True)
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\avg_mpg_ev_hev.png')
plt.show()

# Plot average CO2 emissions over the years for EVs and HEVs
plt.figure(figsize=(14, 8))
for fuel_type in average_co2_ev_hev['fuelType'].unique():
    subset = average_co2_ev_hev[average_co2_ev_hev['fuelType'] == fuel_type]
    plt.plot(subset['year'], subset['co2_emissions'], marker='o', label=fuel_type)
plt.title('Average CO2 Emissions Change from 2014 to 2024 by Fuel Type')
plt.xlabel('Year')
plt.ylabel('Average CO2 Emissions (g/mile)')
plt.legend(title='Fuel Type', loc='upper left', bbox_to_anchor=(1, 1))
plt.grid(True)
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\avg_co2_ev_hev.png')
plt.show()

# Close the connection
engine.dispose()
