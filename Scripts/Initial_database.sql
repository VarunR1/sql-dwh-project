/*
=============================================================
Creating Initial Database  "DataWarehouse".
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
*/

USE master;
Go

-- Create the 'DataWarehouse' Database
CREATE Database DataWarehouse;
Go

USE DataWarehouse;
Go

-- Create Schemas
Create Schema bronze;
Go

Create Schema silver;
Go

Create Schema Gold;
Go
