import pandas as pd
import datetime

# File paths
vehicles_csv = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles_converted.csv'
emissions_csv = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_emissions.csv'
diesel_csv = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\diesel_unified.csv'
gasoline_csv = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\gasoline_unified.csv'
output_sql = r'X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\sql\fact_dimension_tables.sql'

# Read CSV files
vehicles_df = pd.read_csv(vehicles_csv)
emissions_df = pd.read_csv(emissions_csv)
diesel_df = pd.read_csv(diesel_csv)
gasoline_df = pd.read_csv(gasoline_csv)

# Create SQL script
with open(output_sql, 'w') as sql_file:
    # Dimension Tables First
    sql_file.write("-- Dimension Tables\n")

    # Create DimVehicle Table first
    sql_file.write("""
CREATE TABLE DimVehicle (
    VehicleID INT PRIMARY KEY,
    Make NVARCHAR(50),
    Model NVARCHAR(100),
    BaseModel NVARCHAR(50),
    Year INT,
    EngineDescription NVARCHAR(100),
    Displacement FLOAT,
    Cylinders INT,
    DriveType NVARCHAR(50),
    Transmission NVARCHAR(50),
    VehicleClass NVARCHAR(50)
);\n\n
""")

    sql_file.write("""
CREATE TABLE DimFuel (
    FuelID INT IDENTITY(1,1) PRIMARY KEY,
    FuelType NVARCHAR(50),
    FuelPrice FLOAT,
    Date DATE
);\n\n
""")

    sql_file.write("""
CREATE TABLE DimEmissions (
    EmissionID NVARCHAR(50) PRIMARY KEY,
    VehicleID INT,
    EmissionStandard NVARCHAR(50),
    SmogRating INT,
    SalesArea NVARCHAR(50),
    FOREIGN KEY (VehicleID) REFERENCES DimVehicle(VehicleID)
);\n\n
""")

    sql_file.write("""
CREATE TABLE DimTime (
    TimeID INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT
);\n\n
""")

    # Fact Table
    sql_file.write("-- Fact Table\n")
    sql_file.write("""
CREATE TABLE FactFuelEconomy (
    FactID INT IDENTITY(1,1) PRIMARY KEY,
    VehicleID INT,
    FuelCost FLOAT,
    CityMPG FLOAT,
    HighwayMPG FLOAT,
    CombinedMPG FLOAT,
    CO2Emissions FLOAT,
    SmogScore INT,
    Date DATE,
    FOREIGN KEY (VehicleID) REFERENCES DimVehicle(VehicleID)
);\n\n
""")

    # Insert Data into Dimension Tables
    sql_file.write("-- Insert Data into Dimension Tables\n")

    # DimVehicle
    for _, row in vehicles_df.iterrows():
        make = row['make'].replace("'", "''") if pd.notnull(row['make']) else 'NULL'
        model = row['model'].replace("'", "''") if pd.notnull(row['model']) else 'NULL'
        base_model = row['baseModel'].replace("'", "''") if pd.notnull(row['baseModel']) else 'NULL'
        engine_desc = row['eng_dscr'].replace("'", "''") if pd.notnull(row['eng_dscr']) else 'NULL'
        drive_type = row['drive'].replace("'", "''") if pd.notnull(row['drive']) else 'NULL'
        transmission = row['trany'].replace("'", "''") if pd.notnull(row['trany']) else 'NULL'
        vehicle_class = row['VClass'].replace("'", "''") if pd.notnull(row['VClass']) else 'NULL'
        sql_file.write(
            f"INSERT INTO DimVehicle VALUES ({row['id']}, '{make}', '{model}', '{base_model}', "
            f"{row['year'] if pd.notnull(row['year']) else 'NULL'}, "
            f"'{engine_desc}', "
            f"{row['displ'] if pd.notnull(row['displ']) else 'NULL'}, "
            f"{row['cylinders'] if pd.notnull(row['cylinders']) else 'NULL'}, "
            f"'{drive_type}', "
            f"'{transmission}', "
            f"'{vehicle_class}');\n"
        )

    # DimFuel (Gasoline)
    for _, row in gasoline_df.iterrows():
        fuel_price = row['U.S. Regular All Formulations Retail Gasoline Prices $/gal'] if pd.notnull(
            row['U.S. Regular All Formulations Retail Gasoline Prices $/gal']) else 'NULL'
        sql_file.write(
            f"INSERT INTO DimFuel (FuelType, FuelPrice, Date) VALUES ('Regular', {fuel_price}, '{row['Day']}');\n"
        )

    # DimFuel (Diesel)
    for _, row in diesel_df.iterrows():
        fuel_price = row['Price'] if pd.notnull(row['Price']) else 'NULL'
        sql_file.write(
            f"INSERT INTO DimFuel (FuelType, FuelPrice, Date) VALUES ('Diesel', {fuel_price}, '{row['Day']}');\n"
        )

    # DimEmissions
    for _, row in emissions_df.iterrows():
        emission_id = row['efid']
        vehicle_id = row['id']
        emission_standard = row['stdText'].replace("'", "''") if pd.notnull(row['stdText']) else 'NULL'
        smog_rating = row['score'] if pd.notnull(row['score']) else 'NULL'
        sales_area = str(row['salesArea']).replace("'", "''") if pd.notnull(row['salesArea']) else 'NULL'
        sql_file.write(
            f"INSERT INTO DimEmissions VALUES ('{emission_id}', {vehicle_id}, '{emission_standard}', {smog_rating}, '{sales_area}');\n"
        )

    # DimTime
    for _, row in vehicles_df.iterrows():
        try:
            date_str = row['record_created']
            parsed_date = datetime.datetime.strptime(date_str, "%m/%d/%Y")  # Ensure correct format
            year, month, day = parsed_date.year, parsed_date.month, parsed_date.day
            quarter = (month - 1) // 3 + 1
            sql_file.write(f"INSERT INTO DimTime VALUES ('{parsed_date.date()}', {year}, {month}, {day}, {quarter});\n")
        except Exception as e:
            print(f"Error processing date {row['record_created']}: {e}")

    # Fact Table
    sql_file.write("-- Insert Data into Fact Table\n")
    for _, row in vehicles_df.iterrows():
        smog_score = (
            emissions_df[emissions_df['id'] == row['id']]['score'].iloc[0]
            if not emissions_df[emissions_df['id'] == row['id']].empty
            else 'NULL'
        )
        fuel_cost = row['fuelCost08'] if pd.notnull(row['fuelCost08']) else 'NULL'
        city_mpg = row['city08'] if pd.notnull(row['city08']) else 'NULL'
        highway_mpg = row['highway08'] if pd.notnull(row['highway08']) else 'NULL'
        combined_mpg = row['comb08'] if pd.notnull(row['comb08']) else 'NULL'
        co2_emissions = row['co2TailpipeGpm'] if pd.notnull(row['co2TailpipeGpm']) else 'NULL'
        record_date = row['record_created']
        sql_file.write(
            f"INSERT INTO FactFuelEconomy VALUES ({row['id']}, {fuel_cost}, {city_mpg}, "
            f"{highway_mpg}, {combined_mpg}, {co2_emissions}, {smog_score}, '{record_date}');\n"
        )

print(f"SQL script generated at: {output_sql}")
