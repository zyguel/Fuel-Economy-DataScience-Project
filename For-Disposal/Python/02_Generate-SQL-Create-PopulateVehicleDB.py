import pandas as pd

# Load the cleaned dataset
vehicle_df = pd.read_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicle.csv')

# Create dimension tables
dim_year = vehicle_df[['year']].drop_duplicates().reset_index(drop=True)
dim_year['year_id'] = dim_year.index + 1

dim_make = vehicle_df[['make']].drop_duplicates().reset_index(drop=True)
dim_make['make_id'] = dim_make.index + 1

dim_model = vehicle_df[['model', 'VClass']].drop_duplicates().reset_index(drop=True)
dim_model['model_id'] = dim_model.index + 1

dim_engine = vehicle_df[['displ', 'cylinders', 'fuelType']].drop_duplicates().reset_index(drop=True)
dim_engine['engine_id'] = dim_engine.index + 1

# Merge dimension tables to create foreign keys in the fact table
vehicle_df = vehicle_df.merge(dim_year, on='year', how='left')
vehicle_df = vehicle_df.merge(dim_make, on='make', how='left')
vehicle_df = vehicle_df.merge(dim_model, on=['model', 'VClass'], how='left')
vehicle_df = vehicle_df.merge(dim_engine, on=['displ', 'cylinders', 'fuelType'], how='left')

# Create the fact table
fact_vehicle_efficiency = vehicle_df[['id', 'year_id', 'make_id', 'model_id', 'engine_id', 'average_mpg', 'barrels08', 'co2TailpipeGpm', 'ghgScore']]
fact_vehicle_efficiency.rename(columns={'id': 'vehicle_id', 'barrels08': 'fuel_consumption', 'co2TailpipeGpm': 'co2_emissions', 'ghgScore': 'ghg_score'}, inplace=True)

# Function to generate SQL creation scripts for SQL Server 2019 with foreign keys
def generate_sql_create_table(dim_table, table_name, primary_key):
    columns = dim_table.columns
    sql = f"CREATE TABLE {table_name} (\n"
    for col in columns:
        if col == primary_key:
            sql += f"    {col} INT PRIMARY KEY,\n"
        elif col.endswith("_id"):
            sql += f"    {col} INT,\n"
        else:
            sql += f"    {col} VARCHAR(255),\n"
    sql = sql.rstrip(",\n") + "\n);\n"
    return sql

# Generate SQL for dimension tables with primary keys
sql_dim_year = generate_sql_create_table(dim_year, 'dim_year', 'year_id')
sql_dim_make = generate_sql_create_table(dim_make, 'dim_make', 'make_id')
sql_dim_model = generate_sql_create_table(dim_model, 'dim_model', 'model_id')
sql_dim_engine = generate_sql_create_table(dim_engine, 'dim_engine', 'engine_id')

# Generate SQL for fact table with foreign keys
sql_fact_vehicle_efficiency = (
    "CREATE TABLE fact_vehicle_efficiency (\n"
    "    vehicle_id INT PRIMARY KEY,\n"
    "    year_id INT,\n"
    "    make_id INT,\n"
    "    model_id INT,\n"
    "    engine_id INT,\n"
    "    average_mpg FLOAT,\n"
    "    fuel_consumption FLOAT,\n"
    "    co2_emissions FLOAT,\n"
    "    ghg_score INT,\n"
    "    FOREIGN KEY (year_id) REFERENCES dim_year(year_id),\n"
    "    FOREIGN KEY (make_id) REFERENCES dim_make(make_id),\n"
    "    FOREIGN KEY (model_id) REFERENCES dim_model(model_id),\n"
    "    FOREIGN KEY (engine_id) REFERENCES dim_engine(engine_id)\n"
    ");\n"
)

# Function to generate SQL insertion scripts, handling NaN values
def generate_sql_insert_data(dim_table, table_name):
    columns = dim_table.columns
    sql = ""
    for index, row in dim_table.iterrows():
        values = ", ".join([f"NULL" if pd.isna(value) else f"'{value}'" if isinstance(value, str) else str(value) for value in row])
        sql += f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});\n"
    return sql

# Generate SQL for inserting data into dimension tables
sql_insert_dim_year = generate_sql_insert_data(dim_year, 'dim_year')
sql_insert_dim_make = generate_sql_insert_data(dim_make, 'dim_make')
sql_insert_dim_model = generate_sql_insert_data(dim_model, 'dim_model')
sql_insert_dim_engine = generate_sql_insert_data(dim_engine, 'dim_engine')

# Generate SQL for inserting data into fact table
sql_insert_fact_vehicle_efficiency = generate_sql_insert_data(fact_vehicle_efficiency, 'fact_vehicle_efficiency')

# Combine SQL scripts for table creation, data insertion, and database creation
sql_scripts = (
    "CREATE DATABASE VehicleFuelEfficiencyDB;\n"
    "USE VehicleFuelEfficiencyDB;\n\n" +
    sql_dim_year +
    sql_insert_dim_year +
    sql_dim_make +
    sql_insert_dim_make +
    sql_dim_model +
    sql_insert_dim_model +
    sql_dim_engine +
    sql_insert_dim_engine +
    sql_fact_vehicle_efficiency +
    sql_insert_fact_vehicle_efficiency
)

# Save SQL to file
with open(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\sql\star_schema_with_data.sql', 'w') as file:
    file.write(sql_scripts)

print("SQL script for star schema with data and database creation generated successfully.")
