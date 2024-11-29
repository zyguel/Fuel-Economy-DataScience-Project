import pandas as pd
import sqlite3


def create_tables_and_insert_data():
    # Load the CSV files into DataFrames
    vehicles_df = pd.read_csv(r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_vehicles_converted.csv")
    emissions_df = pd.read_csv(r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\cleaned_emissions.csv")
    gasoline_df = pd.read_csv(r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\gasoline_unified.csv")
    diesel_df = pd.read_csv(r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\csv\diesel_unified.csv")

    # Rename columns in gasoline_df
    gasoline_df.rename(columns={
        'U.S. Midgrade All Formulations Retail Gasoline Prices $/gal': 'Midgrade',
        'U.S. Premium All Formulations Retail Gasoline Prices $/gal': 'Premium Gasoline',
        'U.S. Regular All Formulations Retail Gasoline Prices $/gal': 'Regular Gasoline'
    }, inplace=True)

    # Create a SQLite database
    conn = sqlite3.connect('fuel_data.db')
    cursor = conn.cursor()

    # Create dimension tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DimVehicle (
        id INTEGER PRIMARY KEY,
        make TEXT,
        model TEXT,
        mpgData TEXT,
        phevBlended TEXT,
        trany TEXT,
        UCity REAL,
        UHighway REAL,
        VClass TEXT,
        year INTEGER,
        youSaveSpend INTEGER,
        baseModel TEXT,
        createdOn TEXT,
        modifiedOn TEXT,
        startStop TEXT,
        record_created TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DimEmission (
        efid TEXT PRIMARY KEY,
        id INTEGER,
        salesArea INTEGER,
        score INTEGER,
        standard INTEGER,
        stdText TEXT,
        FOREIGN KEY (id) REFERENCES DimVehicle(id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DimGasoline (
        Day TEXT PRIMARY KEY,
        Midgrade REAL,
        PremiumGasoline REAL,
        RegularGasoline REAL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DimDiesel (
        Day TEXT PRIMARY KEY,
        Price REAL
    )
    ''')

    # Create fact table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS FactFuelPrices (
        Day TEXT,
        VehicleID INTEGER,
        EmissionID TEXT,
        DieselPrice REAL,
        GasolineMidgrade REAL,
        GasolinePremium REAL,
        GasolineRegular REAL,
        FOREIGN KEY (VehicleID) REFERENCES DimVehicle(id),
        FOREIGN KEY (EmissionID) REFERENCES DimEmission(efid),
        FOREIGN KEY (Day) REFERENCES DimGasoline(Day),
        FOREIGN KEY (Day) REFERENCES DimDiesel(Day)
    )
    ''')

    # Insert data into dimension tables
    vehicles_df.to_sql('DimVehicle', conn, if_exists='replace', index=False)
    emissions_df.to_sql('DimEmission', conn, if_exists='replace', index=False)
    gasoline_df.to_sql('DimGasoline', conn, if_exists='replace', index=False)
    diesel_df.to_sql('DimDiesel', conn, if_exists='replace', index=False)

    # Merge gasoline and diesel dataframes for the fact table
    fact_df = pd.merge(gasoline_df, diesel_df, on='Day', how='inner')
    fact_df['VehicleID'] = vehicles_df['id'].iloc[0]  # Using first vehicle for simplicity
    fact_df['EmissionID'] = emissions_df['efid'].iloc[0]  # Using first emission record for simplicity

    fact_df = fact_df[['Day', 'VehicleID', 'EmissionID', 'Price', 'Midgrade', 'Premium Gasoline', 'Regular Gasoline']]
    fact_df.columns = ['Day', 'VehicleID', 'EmissionID', 'DieselPrice', 'GasolineMidgrade', 'GasolinePremium',
                       'GasolineRegular']

    # Insert data into fact table
    fact_df.to_sql('FactFuelPrices', conn, if_exists='replace', index=False)

    # Export the database to an SQL file
    sql_file_path = r"X:\Data-Mining-Project\Fuel-Economy-DataScience-Project\sql\initialize_database.sql"
    with open(sql_file_path, 'w') as f:
        for line in conn.iterdump():
            f.write('%s\n' % line)

    # Commit and close the connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_tables_and_insert_data()
