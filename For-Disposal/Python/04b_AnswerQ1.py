import pyodbc
import pandas as pd
import matplotlib.pyplot as plt

# Database connection details
server = 'ZYGUEL'  # Your server name
database = 'VehicleFuelEfficiencyDB'  # Your database name

# Create a connection string for Windows Authentication
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Establish a connection
conn = pyodbc.connect(connection_string)

# Create a cursor object
cursor = conn.cursor()

# Query to get the required data from the fact and dimension tables
query = '''
    SELECT 
        fe.vehicle_id,
        y.year,
        m.VClass,
        fe.average_mpg
    FROM fact_vehicle_efficiency fe
    JOIN dim_year y ON fe.year_id = y.year_id
    JOIN dim_model m ON fe.model_id = m.model_id
'''

# Execute the query and load the data into a DataFrame
df = pd.read_sql_query(query, conn)

# Calculate the average MPG per year and vehicle type
average_mpg_by_year_type = df.groupby(['year', 'VClass'])['average_mpg'].mean().reset_index()

# Plot the average MPG change over the years for different vehicle types
plt.figure(figsize=(14, 8))
for vehicle_type in average_mpg_by_year_type['VClass'].unique():
    subset = average_mpg_by_year_type[average_mpg_by_year_type['VClass'] == vehicle_type]
    plt.plot(subset['year'], subset['average_mpg'], marker='o', label=vehicle_type)

plt.title('Average Fuel Efficiency (MPG) Change from 2014 to 2024 by Vehicle Type')
plt.xlabel('Year')
plt.ylabel('Average MPG')
plt.legend(title='Vehicle Type')
plt.grid(True)

# Save the plot as an image file
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\avg_mpg_change.png')

# Display the plot
plt.show()

# Summary Statistics
summary_stats = average_mpg_by_year_type.groupby('VClass')['average_mpg'].describe()

# Print summary statistics
print(summary_stats)

# Close the connection
conn.close()
