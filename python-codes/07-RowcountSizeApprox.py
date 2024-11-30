import pyodbc

# Set up the connection to SQL Server using Windows Authentication
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=ZYGUEL;'  # Replace with your actual server name
    'DATABASE=VehicleFuelEfficiencyDB;'  # Replace with your actual database name
    'Trusted_Connection=yes;'
)


# Function to execute SQL query and return results
def execute_query(query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()  # Fetch the first row (use fetchall() for all rows)
    return result


# List of tables (dimensions and facts) to check
tables = [
    'FactFuelEconomy', 'DimVehicle', 'DimFuel', 'DimEmissions'
]

# Get row count and approximate size for each table
for table in tables:
    # Fix the query to avoid any issues with COUNT and ensure the correct syntax
    row_count_query = f"SELECT COUNT(*) FROM {table}"
    size_query = f"""
        SELECT 
            SUM(reserved_page_count) * 8 / 1024 AS TableSizeMB
        FROM 
            sys.dm_db_partition_stats 
        WHERE 
            object_id = OBJECT_ID('{table}') AND index_id <= 1
    """

    # Execute queries
    try:
        row_count_result = execute_query(row_count_query)
        size_result = execute_query(size_query)

        # Extract results
        row_count = row_count_result[0]  # Only the first value of the result (count)
        table_size_mb = size_result[0] if size_result else 0  # Handle case where size might be None

        # Print results
        print(f"Table: {table}")
        print(f"Row Count: {row_count}")
        print(f"Approximate Table Size: {table_size_mb:.2f} MB")
        print('-' * 50)
    except Exception as e:
        print(f"Error processing table {table}: {e}")

# Close the connection to the database
conn.close()