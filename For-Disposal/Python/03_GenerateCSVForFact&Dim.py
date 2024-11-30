import pandas as pd
import os

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

# Save dimension tables to CSV
dim_year.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\dim_year.csv', index=False)
dim_make.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\dim_make.csv', index=False)
dim_model.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\dim_model.csv', index=False)
dim_engine.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\dim_engine.csv', index=False)

# Save fact table to CSV
fact_vehicle_efficiency.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\fact_vehicle_efficiency.csv', index=False)

# Function to get file size in MB
def get_file_size(file_path):
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return round(size_mb, 3)

# Calculate physical size
tables = {
    'dim_year': dim_year,
    'dim_make': dim_make,
    'dim_model': dim_model,
    'dim_engine': dim_engine,
    'fact_vehicle_efficiency': fact_vehicle_efficiency
}

table_sizes = {table_name: get_file_size(f'X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/{table_name}.csv') for table_name in tables}

print("Fact and dimension tables have been saved to individual CSV files.")

# Generate Physical Capacity Plan
physical_capacity_plan = pd.DataFrame([
    {'Sr. No.': 1, 'Table Name': 'Dim_Year', 'Table Type': 'Dimension', 'Row Count (Approx)': len(dim_year), 'Physical Size (Approx)': f"{table_sizes['dim_year']} MB"},
    {'Sr. No.': 2, 'Table Name': 'Dim_Make', 'Table Type': 'Dimension', 'Row Count (Approx)': len(dim_make), 'Physical Size (Approx)': f"{table_sizes['dim_make']} MB"},
    {'Sr. No.': 3, 'Table Name': 'Dim_Model', 'Table Type': 'Dimension', 'Row Count (Approx)': len(dim_model), 'Physical Size (Approx)': f"{table_sizes['dim_model']} MB"},
    {'Sr. No.': 4, 'Table Name': 'Dim_Engine', 'Table Type': 'Dimension', 'Row Count (Approx)': len(dim_engine), 'Physical Size (Approx)': f"{table_sizes['dim_engine']} MB"},
    {'Sr. No.': 5, 'Table Name': 'Fact_Vehicle_Efficiency', 'Table Type': 'Fact', 'Row Count (Approx)': len(fact_vehicle_efficiency), 'Physical Size (Approx)': f"{table_sizes['fact_vehicle_efficiency']} MB"}
])

total_size = physical_capacity_plan['Physical Size (Approx)'].str.replace(' MB', '').astype(float).sum()
physical_capacity_plan.loc[len(physical_capacity_plan.index)] = {'Sr. No.': '', 'Table Name': 'Total', 'Table Type': '', 'Row Count (Approx)': '', 'Physical Size (Approx)': f"{total_size} MB"}

print(physical_capacity_plan)

physical_capacity_plan.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\physical_capacity_plan.csv', index=False)
