ALTER TABLE DimVehicle
ADD EffectiveDate DATE DEFAULT GETDATE() NOT NULL;

SELECT COLUMN_NAME, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DimVehicle';

UPDATE DimVehicle
SET EffectiveDate = '2024-01-01' -- Example date
WHERE EffectiveDate = GETDATE(); -- Replace with your condition if needed

ALTER TABLE DimVehicle
ADD EndDate DATE NULL,
    IsCurrent BIT NOT NULL DEFAULT 1;
