/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
Create or Alter Procedure bronze.load_bronze As
Begin
	Declare @start_time DateTime, @end_time DateTime, @batch_start_time DateTime, @batch_end_time DateTime;
	Begin Try
		Set @batch_start_time = GetDate();
		Print Replicate('-',40);
		Print'Starting Bronze Layer Data Load';
		Print Replicate('-', 40);

		Print Replicate('=', 40);
		print('Loading the CRM Tables');
		Print Replicate ('=', 40);

		Set @start_time = GetDate();
			Print'>>> Truncating Table: bronze.crm_cust_info';
			Truncate Table bronze.crm_cust_info;
			Print'>>> Inserting the Data into: bronze.crm_cust_info';
			Bulk Insert bronze.crm_cust_info
			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_crm\cust_info.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		Set @start_time = GetDate();
			Print'>>> Truncating Table: bronze.crm_prd_info';
			Truncate Table bronze.crm_prd_info;
			Print'>>> Inserting the Data into: bronze.crm_prd_info';
			Bulk Insert bronze.crm_prd_info
			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_crm\prd_info.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		Set @start_time = GetDate();
			Print'>>> Truncating Table:  bronze.crm_sales_details';
			Truncate Table bronze.crm_sales_details;
			Print'>>> Inserting the Data into: bronze.crm_sales_details';
			Bulk Insert bronze.crm_sales_details

			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_crm\sales_details.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		
		Print Replicate('=', 40);
		print('Loading the ERP Tables');
		Print Replicate ('=', 40);

		Set @start_time = GetDate();
			Print'>>> Truncating Table: bronze.erp_cust_az12';
			Truncate Table bronze.erp_cust_az12;
			Print'>>> Inserting the Data into: bronze.erp_cust_az12';
			Bulk Insert bronze.erp_cust_az12
			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_erp\CUST_AZ12.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		Set @start_time = GetDate();
			Print'>>> Truncating Table: bronze.erp_loc_a101';
			Truncate Table bronze.erp_loc_a101;
			Print'>>> Inserting the Data into: bronze.erp_loc_a101';
			Bulk Insert bronze.erp_loc_a101
			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_erp\LOC_A101.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		Set @start_time = GetDate();
			Print'>>> Truncating Table: erp_px_cat_g1v2';
			Truncate Table bronze.erp_px_cat_g1v2;
			Print'>>> Inserting the Data into: bronze.erp_px_cat_g1v2';
			Bulk Insert bronze.erp_px_cat_g1v2
			From "C:\Users\lasya\OneDrive\Desktop\DW Project\dw_project\datasets\source_erp\PX_CAT_G1V2.csv"
			With (
					FirstRow = 2, 
					FieldTerminator = ',',
					TabLock
			);
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);

		Set @batch_end_time= GetDate();
		Print Replicate('-', 40)
		print 'Loading the Bronze layer has been Completed'
		Print '  -Total Load Duration: ' + Cast(DateDiff(second, @batch_start_time, @batch_end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 40)
	End Try
	Begin Catch
		Print Replicate('-',40)
		Print 'Error Occured During Loading the Bronze Layer'
		Print 'Error Message' + Error_Message();
		Print 'Error Message' + Cast (Error_Number() as nvarchar);
		Print 'Error Message' + Cast (Error_State() as nvarchar);
		Print Replicate('-',40)
	End Catch
End
