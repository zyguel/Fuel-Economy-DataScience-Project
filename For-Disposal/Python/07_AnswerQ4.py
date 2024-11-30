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
        fe.co2_emissions,
        e.displ,
        e.cylinders,
        e.fuelType,
        y.year
    FROM fact_vehicle_efficiency fe
    JOIN dim_engine e ON fe.engine_id = e.engine_id
    JOIN dim_year y ON fe.year_id = y.year_id
    WHERE y.year BETWEEN 2014 AND 2024
'''

# Execute the query and load the data into a DataFrame
df = pd.read_sql_query(query, engine)

# Calculate average CO2 emissions per year
average_co2_per_year = df.groupby(['year'])['co2_emissions'].mean().reset_index()

# Print average CO2 emissions per year
print("Average CO2 Emissions per Mile from 2014 to 2024:")
print(average_co2_per_year)

# Plot average CO2 emissions per year
plt.figure(figsize=(12, 6))
plt.plot(average_co2_per_year['year'], average_co2_per_year['co2_emissions'], marker='o', linestyle='-', color='b', label='Average CO2 Emissions')
for i in range(len(average_co2_per_year)):
    if average_co2_per_year['year'][i] in [2014, 2018, 2024]:  # Highlight key points
        plt.text(average_co2_per_year['year'][i], average_co2_per_year['co2_emissions'][i], f'{average_co2_per_year["co2_emissions"][i]:.2f}', fontsize=9, ha='right')
plt.title('Average CO2 Emissions per Mile from 2014 to 2024')
plt.xlabel('Year')
plt.ylabel('Average CO2 Emissions (g/mile)')
plt.xticks(average_co2_per_year['year'], rotation=45)  # Ensure all years are visible
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\avg_co2_emissions_per_year.png')
plt.show()

# Calculate CO2 emissions based on fuel type
average_co2_by_fuelType = df.groupby(['fuelType', 'year'])['co2_emissions'].mean().reset_index()

# Print CO2 emissions by fuel type
print("\nAverage CO2 Emissions per Mile by Fuel Type (2014-2024):")
print(average_co2_by_fuelType)

# Plot CO2 emissions by fuel type as line graph
plt.figure(figsize=(12, 6))
for fuel_type in average_co2_by_fuelType['fuelType'].unique():
    subset = average_co2_by_fuelType[average_co2_by_fuelType['fuelType'] == fuel_type]
    plt.plot(subset['year'], subset['co2_emissions'], marker='o', linestyle='-', label=fuel_type)
    for i in range(len(subset)):
        if subset['fuelType'].iloc[i] in ['Midgrade', 'Regular Gas and Electricity', 'Premium Gas or Electricity']:  # Highlight key points
            plt.text(subset['year'].iloc[i], subset['co2_emissions'].iloc[i], f'{subset["co2_emissions"].iloc[i]:.2f}', fontsize=9, ha='right')
plt.title('Average CO2 Emissions per Mile by Fuel Type (2014-2024)')
plt.xlabel('Year')
plt.ylabel('Average CO2 Emissions (g/mile)')
plt.legend(title='Fuel Type', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\co2_emissions_by_fuelType.png')
plt.show()

# Close the connection
engine.dispose()
