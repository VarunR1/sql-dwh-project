/* 
---------------------------------------------
Firstly create the Databases and Schemas
---------------------------------------------
Purpose of this Script:
The Purpose of this script is to create a database called 'Datawarehouse' and
 set seperate schemas in the database for 'bronze', 'silver' and 'gold'.
*/

Use master;

Create Database DataWarehouse;

Use DataWarehouse;

--Create three Schemas

Create Schema bronze;
Go
Create Schema silver;
Go
Create Schema gold;
Go
