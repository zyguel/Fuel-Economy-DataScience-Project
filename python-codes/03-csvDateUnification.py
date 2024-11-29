import pandas as pd
from datetime import datetime


def unify_gasoline_dates(input_path, output_path):
    # Try reading with different encodings
    encodings = ['utf-8', 'latin-1', 'ISO-8859-1']

    for encoding in encodings:
        try:
            # Read the gasoline CSV
            df = pd.read_csv(input_path, encoding=encoding)

            # Convert the 'Day' column to datetime
            df['Day'] = pd.to_datetime(df['Day'], format='%m/%d/%Y')

            # Sort the dataframe by date
            df = df.sort_values('Day')

            # Save the updated CSV
            df.to_csv(output_path, index=False, encoding='utf-8')

            return df
        except UnicodeDecodeError:
            continue

    raise ValueError("Could not read the file with any of the attempted encodings")


def unify_diesel_dates(input_path, output_path):
    # Try reading with different encodings
    encodings = ['utf-8', 'latin-1', 'ISO-8859-1']

    for encoding in encodings:
        try:
            # Read the diesel CSV
            df = pd.read_csv(input_path, encoding=encoding)

            # Melt the dataframe to convert from wide to long format
            df_melted = df.melt(id_vars=['Year'], var_name='Month', value_name='Price')

            # Create a datetime column
            month_map = {
                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
            }
            df_melted['Day'] = df_melted.apply(
                lambda row: datetime(int(row['Year']), month_map[row['Month']], 1),
                axis=1
            )

            # Drop unnecessary columns and sort
            df_processed = df_melted[['Day', 'Price']].sort_values('Day')

            # Save the updated CSV
            df_processed.to_csv(output_path, index=False, encoding='utf-8')

            return df_processed
        except UnicodeDecodeError:
            continue

    raise ValueError("Could not read the file with any of the attempted encodings")


# File paths (update these to your specific paths)
gasoline_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/gasoline.csv"
diesel_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/diesel.csv"

# Output paths
gasoline_output = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/gasoline_unified.csv"
diesel_output = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/diesel_unified.csv"

# Process files
gasoline_df = unify_gasoline_dates(gasoline_csv, gasoline_output)
diesel_df = unify_diesel_dates(diesel_csv, diesel_output)

print("Gasoline CSV processed. Rows:", len(gasoline_df))
print("Diesel CSV processed. Rows:", len(diesel_df))