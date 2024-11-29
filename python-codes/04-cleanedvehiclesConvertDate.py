import pandas as pd

def convert_date_format(input_path, output_path):
    # Read the CSV file
    df = pd.read_csv(input_path)

    # Define a dictionary to map month abbreviations to digits
    month_mapping = {
        'Jan': '1',
        'Feb': '2',
        'Mar': '3',
        'Apr': '4',
        'May': '5',
        'Jun': '6',
        'Jul': '7',
        'Aug': '8',
        'Sep': '9',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }

    # Function to extract and convert date
    def extract_date(date_str):
        try:
            print(f"Extracting date: {date_str}")
            parts = date_str.split()
            month = month_mapping[parts[1]]
            day = parts[2]
            year = parts[5]
            extracted_date = f"{month}/{day}/{year}"
            print(f"Extracted date: {extracted_date}")
            return extracted_date
        except Exception as e:
            print(f"Failed to extract date: {date_str}, error: {e}")
            return date_str

    # Apply the date extraction to the createdOn column
    df['record_created'] = df['createdOn'].apply(extract_date)

    # Save the updated DataFrame
    df.to_csv(output_path, index=False)

    return df

# File paths (update these to your specific paths)
input_csv = r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles.csv"
output_csv = r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles_converted.csv"

# Process the file
converted_df = convert_date_format(input_csv, output_csv)

print("CSV processed. Rows:", len(converted_df))
