import pyodbc
import pandas as pd
import seaborn as sns
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
        fe.average_mpg,
        e.displ,
        e.cylinders
    FROM fact_vehicle_efficiency fe
    JOIN dim_engine e ON fe.engine_id = e.engine_id
    JOIN dim_year y ON fe.year_id = y.year_id
    WHERE y.year BETWEEN 2014 AND 2024
'''

# Execute the query and load the data into a DataFrame
df = pd.read_sql_query(query, conn)

# Calculate the correlation matrix
correlation_matrix = df[['displ', 'cylinders', 'average_mpg']].corr()

# Plot the correlation matrix
plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Correlation Matrix: Engine Size, Cylinders, and Fuel Efficiency (MPG)')
plt.savefig(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\plots\correlation_matrix.png')
plt.show()

# Close the connection
conn.close()

# Print the correlation matrix
print(correlation_matrix)
