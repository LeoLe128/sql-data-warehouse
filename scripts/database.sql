/*
========================================
Create database and schema
========================================
Purpose:
This script is to create a new database call 'datawarehouse' after checking if it already exists. 
The script add sets up 3 schemas within the database: 'bronze', 'silver', 'gold'.

WARNING!
Running this script will drop the entire database along, which means all the data in the database will be deleted permanently.
Proceed with caution and ensure you have backup before running this script.
*/
USE master;
GO
--Drop and recreate the 'datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'datawarehouse')
BEGIN
  ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE datawarehouse;
END;
GO

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;
GO

USE datawarehouse;
GO

--Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
