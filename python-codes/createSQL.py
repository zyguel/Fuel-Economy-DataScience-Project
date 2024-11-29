import pandas as pd

# Define your CSV file paths
vehicle_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/cleaned_vehicles.csv"
emissions_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/emissions.csv"
gasoline_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/gasoline.csv"
diesel_csv = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/csv/diesel.csv"

# Load data into DataFrames with specified encoding
vehicle_df = pd.read_csv(vehicle_csv, encoding='ISO-8859-1')
emissions_df = pd.read_csv(emissions_csv, encoding='ISO-8859-1')
gasoline_df = pd.read_csv(gasoline_csv, encoding='ISO-8859-1')
diesel_df = pd.read_csv(diesel_csv, encoding='ISO-8859-1')

# Trim whitespace from column names and values
diesel_df.columns = diesel_df.columns.str.strip()
for col in diesel_df.columns:
    if diesel_df[col].dtype == 'object':
        diesel_df[col] = diesel_df[col].str.strip()

# Reformat diesel data to match gasoline format
diesel_df = diesel_df.melt(id_vars=["Year"], var_name="Month", value_name="Price")
diesel_df["Month"] = diesel_df["Month"].str.strip()  # Ensure no extra spaces
diesel_df["Date"] = pd.to_datetime(diesel_df["Year"].astype(str) + "-" + diesel_df["Month"] + "-01", format="%Y-%b-%d", errors='coerce')

# Convert gasoline day to date format and extract year
gasoline_df["Date"] = pd.to_datetime(gasoline_df["Day"], format="%m/%d/%Y", errors='coerce')
gasoline_df["Year"] = gasoline_df["Date"].dt.year

# Calculate average gasoline prices per year for Regular and Premium Gasoline
average_gasoline_prices = gasoline_df.groupby("Year").mean().reset_index()

# Calculate average diesel prices per year
diesel_df["Year"] = diesel_df["Date"].dt.year
average_diesel_prices = diesel_df.groupby("Year")["Price"].mean().reset_index()

# Merge average fuel prices with vehicle data based on year and fuel type
vehicle_df = vehicle_df.merge(average_gasoline_prices, on="Year", how="left")
vehicle_df = vehicle_df.merge(average_diesel_prices, on="Year", how="left", suffixes=("_gasoline", "_diesel"))

# Replace NaN values with None for SQL insertion
vehicle_df = vehicle_df.where(pd.notnull(vehicle_df), None)
emissions_df = emissions_df.where(pd.notnull(emissions_df), None)
gasoline_df = gasoline_df.where(pd.notnull(gasoline_df), None)
diesel_df = diesel_df.where(pd.notnull(diesel_df), None)

# Define SQL templates for creating tables
create_dimension_emissions = """
CREATE TABLE DimensionEmissions (
    id INT PRIMARY KEY,
    efid VARCHAR(255),
    salesArea VARCHAR(50),
    score INT,
    scoreAlt INT,
    smartwayScore INT,
    standard VARCHAR(50),
    stdText VARCHAR(255)
);
"""

create_dimension_vehicle = """
CREATE TABLE DimensionVehicle (
    id INT PRIMARY KEY,
    make VARCHAR(255),
    model VARCHAR(255),
    cylinders INT,
    displ FLOAT,
    drive VARCHAR(50),
    eng_dscr VARCHAR(255),
    fuelType VARCHAR(50),
    fuelType1 VARCHAR(50),
    trany VARCHAR(50),
    VClass VARCHAR(50),
    baseModel VARCHAR(255),
    createdOn DATE,
    modifiedOn DATE,
    startStop CHAR(1)
);
"""

create_fact_table = """
CREATE TABLE FactVehicle (
    id INT PRIMARY KEY,
    barrels08 FLOAT,
    city08 INT,
    co2TailpipeGpm FLOAT,
    comb08 INT,
    fuelCost08 FLOAT,
    highway08 INT,
    UCity FLOAT,
    UHighway FLOAT,
    youSaveSpend FLOAT,
    year INT,
    emissions_id INT,
    vehicle_id INT,
    avgRegularGasPrice FLOAT,
    avgPremiumGasPrice FLOAT,
    avgDieselPrice FLOAT,
    CONSTRAINT fk_emissions
        FOREIGN KEY (emissions_id) 
        REFERENCES DimensionEmissions(id),
    CONSTRAINT fk_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES DimensionVehicle(id)
);
"""

create_dimension_gasoline = """
CREATE TABLE DimensionGasoline (
    Date DATE PRIMARY KEY,
    MidgradePrice FLOAT,
    PremiumPrice FLOAT,
    RegularPrice FLOAT
);
"""

create_dimension_diesel = """
CREATE TABLE DimensionDiesel (
    Date DATE PRIMARY KEY,
    Price FLOAT
);
"""

# Define SQL templates for inserting data
insert_fact_vehicle = """
INSERT INTO FactVehicle (id, barrels08, city08, co2TailpipeGpm, comb08, fuelCost08, highway08, UCity, UHighway, youSaveSpend, year, emissions_id, vehicle_id, avgRegularGasPrice, avgPremiumGasPrice, avgDieselPrice)
VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});
"""

insert_dimension_vehicle = """
INSERT INTO DimensionVehicle (id, make, model, cylinders, displ, drive, eng_dscr, fuelType, fuelType1, trany, VClass, baseModel, createdOn, modifiedOn, startStop)
VALUES ({}, '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');
"""

insert_dimension_emissions = """
INSERT INTO DimensionEmissions (id, efid, salesArea, score, scoreAlt, smartwayScore, standard, stdText)
VALUES ({}, '{}', '{}', {}, {}, {}, '{}', '{}');
"""

insert_dimension_gasoline = """
INSERT INTO DimensionGasoline (Date, MidgradePrice, PremiumPrice, RegularPrice)
VALUES ('{}', {}, {}, {});
"""

insert_dimension_diesel = """
INSERT INTO DimensionDiesel (Date, Price)
VALUES ('{}', {});
"""

# Generate SQL statements for inserting data
insert_statements = []

# Populate the Dimension tables first
for _, row in emissions_df.iterrows():
    insert_statements.append(insert_dimension_emissions.format(
        row['id'], row['efid'], row['salesArea'], row['score'], row['scoreAlt'], row['smartwayScore'],
        row['standard'], row['stdText']
    ).replace('nan', 'NULL').replace('None', 'NULL'))

for _, row in vehicle_df.iterrows():
    insert_statements.append(insert_dimension_vehicle.format(
        row['id'], row['make'], row['model'], row['cylinders'], row['displ'], row['drive'], row['eng_dscr'],
        row['fuelType'], row['fuelType1'], row['trany'], row['VClass'], row['baseModel'], row['createdOn'],
        row['modifiedOn'], row['startStop']
    ).replace('nan', 'NULL').replace('None', 'NULL'))

for _, row in gasoline_df.iterrows():
    insert_statements.append(insert_dimension_gasoline.format(
        row['Date'].strftime('%Y-%m-%d'), row['U.S. Midgrade All Formulations Retail Gasoline Prices $/gal'],
        row['U.S. Premium All Formulations Retail Gasoline Prices $/gal'],
        row['U.S. Regular All Formulations Retail Gasoline Prices $/gal']
    ).replace('nan', 'NULL').replace('None', 'NULL'))

for _, row in diesel_df.iterrows():
    insert_statements.append(insert_dimension_diesel.format(
        row['Date'].strftime('%Y-%m-%d'), row['Price']
    ).replace('nan', 'NULL').replace('None', 'NULL'))

# Populate the Fact table after the dimensions
for _, row in vehicle_df.iterrows():
    insert_statements.append(insert_fact_vehicle.format(
        row['id'], row['barrels08'], row['city08'], row['co2TailpipeGpm'], row['comb08'], row['fuelCost08'],
        row['highway08'], row['UCity'], row['UHighway'], row['youSaveSpend'], row['year'], row['engId'], row['id'],
        row['U.S. Regular All Formulations Retail Gasoline Prices $/gal'], row['U.S. Premium All Formulations Retail Gasoline Prices $/gal'], row['Price']
    ).replace('nan', 'NULL').replace('None', 'NULL'))

# Save SQL statements to a local file
output_file_path = "X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/sql/create_and_populate_tables.sql"
with open(output_file_path, "w") as file:
    file.write(create_dimension_emissions + "\n\n")
    file.write(create_dimension_vehicle + "\n\n")
    file.write(create_fact_table + "\n\n")
    file.write(create_dimension_gasoline + "\n\n")
    file.write(create_dimension_diesel + "\n\n")
    for statement in insert_statements:
        file.write(statement + "\n")

print(f"SQL statements have been saved to '{output_file_path}'")
