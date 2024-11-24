import pandas as pd
import numpy as np

# Load the dataset
vehicle_df = pd.read_csv(
    r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\vehicles.csv',  # or 'X:/Data-Mining-Project/vehicles.csv'
    dtype={'c240Dscr': 'str', 'c240bDscr': 'str', 'mfrCode': 'str', 'fuelType2': 'str'},
    low_memory=False
)

# Initial inspection
print("Initial shape:", vehicle_df.shape)
print(vehicle_df.head())

# Filter data for the years 2014 to 2024
vehicle_df = vehicle_df[(vehicle_df['year'] >= 2014) & (vehicle_df['year'] <= 2025)]
print("After year filtering:", vehicle_df.shape)
print(vehicle_df.head())

# Drop columns where all values are 0, -1, or NaN (empty)
vehicle_df.replace({0: np.nan, -1: np.nan}, inplace=True)
vehicle_df.dropna(axis=1, how='all', inplace=True)
print("After dropping empty columns:", vehicle_df.shape)
print(vehicle_df.head())

# Drop columns that are not well filled (e.g., less than 60% filled)
threshold = len(vehicle_df) * 0.6
vehicle_df.dropna(thresh=threshold, axis=1, inplace=True)
print("After dropping poorly filled columns:", vehicle_df.shape)
print(vehicle_df.head())

# Handle missing values
missing_values = vehicle_df.isnull().sum()
print("Missing values:\n", missing_values)

# Drop 'co2' and 'co2A' columns
columns_to_drop = ['co2', 'co2A']
vehicle_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
print("After dropping 'co2' and 'co2A' columns:", vehicle_df.shape)
print(vehicle_df.head())

# Update list of columns to drop if they exist
columns_to_drop = ['eng_dscr', 'evMotor', 'mfrCode']
vehicle_df.drop(columns=[col for col in columns_to_drop if col in vehicle_df.columns], inplace=True)
print("After dropping irrelevant columns:", vehicle_df.shape)
print(vehicle_df.head())

# Fill or drop remaining missing values without using chained assignment
if 'fuelType1' in vehicle_df.columns:
    vehicle_df.dropna(subset=['fuelType1'], inplace=True)
print("After handling remaining missing values:", vehicle_df.shape)
print(vehicle_df.head())

# Convert data types
if 'year' in vehicle_df.columns:
    vehicle_df['year'] = vehicle_df['year'].astype(int)

# Remove duplicates
vehicle_df.drop_duplicates(inplace=True)
print("After removing duplicates:", vehicle_df.shape)
print(vehicle_df.head())

# Feature engineering for MPG analysis
if 'city08' in vehicle_df.columns and 'highway08' in vehicle_df.columns:
    vehicle_df['average_mpg'] = vehicle_df[['city08', 'highway08']].mean(axis=1)
print("After feature engineering:", vehicle_df.shape)
print(vehicle_df.head())

# Save cleaned data to the same directory as vehicle.csv
vehicle_df.to_csv(r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicle.csv', index=False)  # or 'X:/Data-Mining-Project/cleaned_vehicle.csv'
print("Final shape of cleaned data:", vehicle_df.shape)
