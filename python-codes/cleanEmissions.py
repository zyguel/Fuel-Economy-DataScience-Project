import pandas as pd

# Load the CSV file with low_memory=False to handle mixed types
file_path = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\emissions.csv'
df = pd.read_csv(file_path, low_memory=False)

# Replace -1, 0, and empty strings with NaN
df.replace([0, -1, ''], pd.NA, inplace=True)

# Drop columns containing only NaN values
df.dropna(axis=1, how='all', inplace=True)

# Drop columns that are 50% or more not populated
threshold = len(df) * 0.5
df.dropna(thresh=threshold, axis=1, inplace=True)

# Save the cleaned DataFrame to a new CSV file
cleaned_file_path = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_emissions.csv'
df.to_csv(cleaned_file_path, index=False)

print(f"Cleaning complete. The cleaned data has been saved to {cleaned_file_path}.")
