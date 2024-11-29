import pandas as pd

# File path (update this to your specific path)
input_csv = r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles_converted.csv"

# Read the CSV file
df = pd.read_csv(input_csv)

# Get distinct values of the 'fuelType1' column
distinct_fuel_types = df['fuelType1'].unique()

# Print distinct fuel types
print("Distinct fuel types in 'fuelType1':")
for fuel_type in distinct_fuel_types:
    print(fuel_type)
