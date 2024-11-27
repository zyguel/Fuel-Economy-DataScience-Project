import pandas as pd
import matplotlib.pyplot as plt
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
        dm.make AS manufacturer_name,
        fe.average_mpg,
        dy.year
    FROM fact_vehicle_efficiency fe
    JOIN dim_make dm ON fe.make_id = dm.make_id
    JOIN dim_year dy ON fe.year_id = dy.year_id
    WHERE dy.year BETWEEN 2014 AND 2024
'''

# Execute the query and load the data into a DataFrame
df = pd.read_sql_query(query, engine)

# Calculate the average fuel efficiency for each manufacturer over the past 10 years
average_efficiency = df.groupby('manufacturer_name')['average_mpg'].mean().reset_index()

# Sort manufacturers by average fuel efficiency
top_manufacturers = average_efficiency.sort_values(by='average_mpg', ascending=False)

# Print the top manufacturers
print("Top Manufacturers by Average Fuel Efficiency (2014-2024):")
print(top_manufacturers)

# Plot the results
plt.figure(figsize=(14, 8))
plt.bar(top_manufacturers['manufacturer_name'], top_manufacturers['average_mpg'], color='skyblue')
plt.xlabel('Manufacturer')
plt.ylabel('Average Fuel Efficiency (MPG)')
plt.title('Top Manufacturers by Average Fuel Efficiency (2014-2024)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\top_manufacturers_fuel_efficiency.png')
plt.show()

# Close the connection
engine.dispose()
