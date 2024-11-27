import pandas as pd

# Load the CSV file with low_memory=False to handle mixed types
file_path = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\vehicles.csv'
df = pd.read_csv(file_path, low_memory=False)

# Replace 0, -1, and empty strings with NaN
df.replace([0, -1, ''], pd.NA, inplace=True)

# Fill NaN values in 'startStop' column with 'N' if it exists
if 'startStop' in df.columns:
    df['startStop'] = df['startStop'].fillna('N')

# Drop columns containing only NaN values
df.dropna(axis=1, how='all', inplace=True)

# Drop columns that are 60% or more not populated
threshold = len(df) * 0.6
df = df.loc[:, df.notnull().sum() > threshold]

# Re-add 'startStop' column if it was dropped and fill NaN with 'N'
if 'startStop' in df.columns:
    df['startStop'] = df['startStop'].fillna('N')
elif 'startStop' in df.columns:
    df = pd.concat([df, pd.Series(['N'] * len(df), name='startStop')], axis=1)

# Keep columns that were initially mentioned if they are present in the dataframe
columns_to_keep = [
    'barrels08', 'city08', 'city08U', 'co2TailpipeGpm', 'comb08', 'comb08U',
    'cylinders', 'displ', 'drive', 'engId', 'eng_dscr', 'fuelCost08', 'fuelType',
    'fuelType1', 'ghgScore', 'highway08', 'highway08U', 'id', 'make', 'model',
    'mpgData', 'phevBlended', 'trany', 'UCity', 'UHighway', 'VClass', 'year',
    'youSaveSpend', 'baseModel', 'createdOn', 'modifiedOn', 'startStop',
    'average_mpg'
]
columns_to_keep = [col for col in columns_to_keep if col in df.columns]
df = df[columns_to_keep]

# Save the cleaned DataFrame to a new CSV file
cleaned_file_path = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles.csv'
df.to_csv(cleaned_file_path, index=False)

print(f"Cleaning complete. The cleaned data has been saved to {cleaned_file_path}.")
