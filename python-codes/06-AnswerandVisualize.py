import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set up the connection to SQL Server using Windows Authentication
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=ZYGUEL;'
    'DATABASE=VehicleFuelEfficiencyDB;'  # Replace with your actual database name
    'Trusted_Connection=yes;'
)

# Function to execute SQL query and return results as DataFrame
def execute_query(query):
    return pd.read_sql(query, conn)

# Q1: Average fuel cost and CO₂ emissions across all vehicles by year
q1_query = """
SELECT 
    YEAR(Date) AS Year,
    AVG(FuelCost) AS AvgFuelCost,
    AVG(CO2Emissions) AS AvgCO2Emissions
FROM 
    FactFuelEconomy
GROUP BY 
    YEAR(Date)
ORDER BY 
    Year;
"""
q1_result = execute_query(q1_query)

# Print the results for Q1
print("Q1: Average Fuel Cost and CO2 Emissions by Year:")
print(q1_result)

# Visualize Q1 results
plt.figure(figsize=(10,6))
sns.lineplot(data=q1_result, x='Year', y='AvgFuelCost', label='Avg Fuel Cost', marker='o')
sns.lineplot(data=q1_result, x='Year', y='AvgCO2Emissions', label='Avg CO2 Emissions', marker='o')
plt.title('Average Fuel Cost and CO2 Emissions Over Time')
plt.xlabel('Year')
plt.ylabel('Values')
plt.legend()
plt.grid(True)
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/q1_avg_fuel_cost_co2.png')
plt.show()

# Q2: Manufacturers with highest average fuel economy (Combined MPG)
q2_query = """
SELECT 
    d.make AS Manufacturer,
    AVG(f.CombinedMPG) AS AvgCombinedMPG
FROM 
    FactFuelEconomy f
JOIN 
    DimVehicle d ON f.VehicleID = d.VehicleID
GROUP BY 
    d.make
ORDER BY 
    AvgCombinedMPG DESC;
"""
q2_result = execute_query(q2_query)

# Print the results for Q2
print("Q2: Manufacturers with Highest Average Fuel Economy (Combined MPG):")
print(q2_result)

# Visualize Q2 results
plt.figure(figsize=(15, 30))  # Increase figure size to make space for the labels
sns.barplot(data=q2_result, x='AvgCombinedMPG', y='Manufacturer', palette='viridis')

# Title and labels
plt.title('Top Manufacturers by Average Combined MPG')
plt.xlabel('Average Combined MPG')
plt.ylabel('Manufacturer')

# Rotate x-axis labels to make them readable
plt.xticks(rotation=90)

# Add grid for better visibility
plt.grid(True)

# Save the figure
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/q2_avg_combined_mpg.png')

# Show the plot
plt.tight_layout()  # Ensure everything fits without overlap
plt.show()

# Q3: Average fuel cost of each fuel type ranked over time
q3_query = """
WITH FuelRank AS (
    SELECT 
        FuelType,
        Date,
        AVG(FuelPrice) AS AvgFuelPrice,
        RANK() OVER (PARTITION BY FuelType ORDER BY Date) AS FuelRank
    FROM 
        DimFuel
    GROUP BY 
        FuelType, Date
)
SELECT 
    FuelType,
    Date,
    AvgFuelPrice,
    FuelRank
FROM 
    FuelRank
ORDER BY 
    Date, FuelType;
"""
q3_result = execute_query(q3_query)

# Print the results for Q3
print("Q3: Average Fuel Cost by Fuel Type Over Time:")
print(q3_result)

# Visualize Q3 results
plt.figure(figsize=(10,6))
sns.lineplot(data=q3_result, x='Date', y='AvgFuelPrice', hue='FuelType', marker='o')
plt.title('Average Fuel Cost by Fuel Type Over Time')
plt.xlabel('Date')
plt.ylabel('Average Fuel Price')
plt.legend(title='Fuel Type')
plt.grid(True)
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/q3_avg_fuel_cost.png')
plt.show()

# Q4: Distribution of vehicle types across manufacturers and their average emissions
q4_query = """
SELECT 
    d.make AS Manufacturer,
    AVG(e.SmogRating) AS AvgEmissionsScore
FROM 
    DimVehicle d
JOIN 
    FactFuelEconomy f ON d.VehicleID = f.VehicleID
JOIN 
    DimEmissions e ON d.VehicleID = e.VehicleID
GROUP BY 
    d.make
ORDER BY 
    AvgEmissionsScore DESC;
"""
q4_result = execute_query(q4_query)

# Print the results for Q4
print("Q4: Distribution of Vehicle Types and Average Emissions:")
print(q4_result)

# Visualize Q4 results
plt.figure(figsize=(15, 30))  # Increase figure size to make space for the labels
sns.barplot(data=q4_result, x='AvgEmissionsScore', y='Manufacturer', palette='coolwarm')

# Title and labels
plt.title('Average Emissions Score by Manufacturer')
plt.xlabel('Average Emissions Score')
plt.ylabel('Manufacturer')

# Rotate y-axis labels to make them readable
plt.xticks(rotation=90)

# Add grid for better visibility
plt.grid(True)

# Save the figure
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/q4_avg_emissions_score.png')

# Show the plot
plt.tight_layout()  # Ensure everything fits without overlap
plt.show()

# Additional Business Question 1: How has the average fuel economy (Combined MPG) for each vehicle class changed over time?
additional_q1_query = """
SELECT 
    YEAR(f.Date) AS Year,
    v.VehicleClass AS VehicleClass,
    AVG(f.CombinedMPG) AS AvgCombinedMPG
FROM 
    FactFuelEconomy f
JOIN 
    DimVehicle v ON f.VehicleID = v.VehicleID
GROUP BY 
    YEAR(f.Date), v.VehicleClass
ORDER BY 
    Year, VehicleClass;
"""
additional_q1_result = execute_query(additional_q1_query)

# Print the results for Additional Question 1
print("Additional Q1: Average Fuel Economy (Combined MPG) by Vehicle Class Over Time:")
print(additional_q1_result)

# Visualize Additional Question 1 results
plt.figure(figsize=(15, 8))  # Increase figure size to provide more space for the plot and legend
sns.lineplot(data=additional_q1_result, x='Year', y='AvgCombinedMPG', hue='VehicleClass', marker='o')

# Title and labels
plt.title('Average Fuel Economy by Vehicle Class Over Time')
plt.xlabel('Year')
plt.ylabel('Average Combined MPG')

# Adjust the legend position to prevent cropping
plt.legend(title='Vehicle Class', bbox_to_anchor=(1.05, 1), loc='upper left')

# Add grid for better visibility
plt.grid(True)

# Save the figure
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/additional_q1_avg_combined_mpg.png', bbox_inches='tight')

# Show the plot
plt.show()

# Additional Business Question 2: Which vehicle classes have shown the greatest improvement in reducing CO₂ emissions per mile over the last 5 years?
additional_q2_query = """
WITH EmissionsByYear AS (
    SELECT 
        YEAR(f.Date) AS Year,
        v.VehicleClass AS VehicleClass,
        AVG(f.CO2Emissions) AS AvgCO2Emissions
    FROM 
        FactFuelEconomy f
    JOIN 
        DimVehicle v ON f.VehicleID = v.VehicleID
    WHERE 
        YEAR(f.Date) >= YEAR(GETDATE()) - 5
    GROUP BY 
        YEAR(f.Date), v.VehicleClass
)
SELECT 
    e1.VehicleClass,
    ((e1.AvgCO2Emissions - e2.AvgCO2Emissions) / e2.AvgCO2Emissions) * 100 AS PercentageReduction
FROM 
    EmissionsByYear e1
JOIN 
    EmissionsByYear e2 ON e1.VehicleClass = e2.VehicleClass 
    AND e1.Year = e2.Year + 5
ORDER BY 
    PercentageReduction DESC;
"""
additional_q2_result = execute_query(additional_q2_query)

# Print the results for Additional Question 2
print("Additional Q2: Vehicle Classes with Greatest CO₂ Emissions Reduction:")
print(additional_q2_result)

# Visualize Additional Question 2 results
plt.figure(figsize=(25,8))
sns.barplot(data=additional_q2_result, x='PercentageReduction', y='VehicleClass', palette='coolwarm')
plt.title('Greatest CO₂ Emissions Reduction by Vehicle Class')
plt.xlabel('Percentage Reduction in CO₂ Emissions')
plt.ylabel('Vehicle Class')
plt.grid(True)
plt.savefig('X:/Data-Mining-Project/Fuel-Economy-DataScience-Project/plots/additional_q2_co2_reduction.png')
plt.show()

# Close the connection to the database
conn.close()
