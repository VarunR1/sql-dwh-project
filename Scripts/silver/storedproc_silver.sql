/*
===============================================================================
Stored Procedure: Transform and Load Data to Silver Layer (Bronze → Silver)
===============================================================================
Description:
    This stored procedure executes the ETL (Extract, Transform, Load) workflow 
    to move data from the 'bronze' schema to the 'silver' schema. 

Operations Performed:
    - Clears existing data from Silver tables.
    - Transforms, cleanses, and inserts data from Bronze into Silver tables.

Parameters:
    None. 
    This procedure does not take any input parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

Create or Alter Procedure silver.load_silver As
Begin
	Declare @start_time DateTime, @end_time DateTime, @batch_start_time DateTime, @batch_end_time DateTime;
	Begin Try
		Set @batch_start_time = GetDate();
		Print Replicate('-',40);
		Print'Loading the Silver Layer Data';
		Print Replicate('-', 40);

		Print Replicate('=', 40);
		print('Loading the CRM Tables');
		Print Replicate ('=', 40);

			--Loading the silver.crm_cust_info
		Set @start_time = GetDate();
			Print '>> Truncating Table: silver.crm_cust_info';
			Truncate Table silver.crm_cust_info;
			Print '>> Inserting Data Into: silver.crm_cust_info';
		Insert Into silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		Select
			cst_id,
			cst_key,
			Trim(cst_firstname) AS cst_firstname,
			Trim(cst_lastname) AS cst_lastname,
			Case 
				When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
				When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
				Else 'n/a'
			End AS cst_marital_status, -- Normalize marital status values to readable format
			Case 
				When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
				When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
				Else 'n/a'
			End AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		From (
			Select
				*,
				Row_Number() Over (Partition By cst_id Order By cst_create_date Desc) AS flag_last
				From bronze.crm_cust_info
				Where cst_id IS NOT NULL
		) t
			Where flag_last = 1; -- Select the most recent record per customer
		Set @end_time = GetDate();
	Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
	Print Replicate('-', 20);
	
		--Loading the silver.crm_prd_info
	Set @start_time = GetDate();
		Print '>> Truncating Table: silver.crm_prd_info';
		Truncate Table silver.crm_cust_info;
		Print '>> Inserting Data Into: silver.crm_prd_info';
		Insert Into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt     
    )
	Select
			prd_id,
			Replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
			SUBSTRING(prd_key, 7, Len(prd_key)) as prd_key, 
			prd_nm,
			IsNull(prd_cost, 0) As prd_cost,
			Case Upper(Trim(prd_line))
				 When 'M' Then 'Mountain'
				 When 'R' Then 'Road'
				 When 'S' Then 'Other Sales'
				 When 'T' Then 'Touring'
				 Else 'n/a'
			End as prd_line,
			Cast(prd_start_dt As Date) As prd_start_dt,
			Cast(Lead(prd_start_dt) Over (Partition By prd_key Order By prd_start_dt)-1 As Date) As prd_end_dt
		From bronze.crm_prd_info;
	Set @end_time = GetDate();
	Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
	Print Replicate('-', 20);

		--Loading the crm_sales_details
	Set @start_time = GetDate();
	Print '>> Truncating Table: silver.crm_sales_details';
	Truncate Table silver.crm_cust_info;
	Print '>> Inserting Data Into: silver.crm_sales_details';
		Insert Into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			Case When sls_order_dt = 0 Or Len(sls_order_dt) !=8 Then Null
				 Else Cast(Cast(sls_order_dt As varchar) As Date)
			End As sls_order_Dt,
			Case When sls_ship_dt = 0 Or Len(sls_ship_dt) !=8 Then Null
				 Else Cast(Cast(sls_ship_dt As varchar) As Date)
			End As sls_ship_dt,
			Case When sls_due_dt = 0 Or Len(sls_due_dt) !=8 Then Null
				 Else Cast(Cast(sls_due_dt As varchar) As Date)
			End As sls_due_dt,
			Case When sls_sales Is Null Or sls_sales <= 0 Or sls_sales != sls_quantity * Abs(sls_price)
					Then sls_quantity *Abs(sls_price)
				 Else sls_sales
			End As sls_sales,
			sls_quantity,
			Case When sls_price Is Null Or sls_price <= 0
					Then sls_sales / NullIf(sls_quantity,0)
				 Else sls_price
			End As sls_price
		From bronze.crm_sales_details;
		Set @end_time = GetDate();
		Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
		Print Replicate('-', 20);


		--Loading the erp_cust_az12 
	Set @start_time = GetDate();
	Print '>> Truncating Table: silver.erp_cust_az12';
	Truncate Table silver.crm_cust_info;
	Print '>> Inserting Data Into: silver.erp_cust_az12';

		Insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen)
		Select 
			Case When cid Like 'NAS%' Then SUBSTRING(cid, 4, Len(cid))
				 Else cid
			End cid,
			Case When bdate> GETDATE() Then Null
				 Else bdate
			End As bdate,
			Case When Upper(Trim(gen)) In ('F', 'Female') Then 'Female'
				 When Upper(Trim(gen)) In ('M', 'Male') Then 'Male'
				 Else 'n/a'
			End As gen
		From bronze.erp_cust_az12;
	Set @end_time = GetDate();
	Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
	Print Replicate('-', 20);


		--loading the erp_loc_a101
	Set @start_time = GetDate();
	Print '>> Truncating Table: silver.erp_loc_a101';
	Truncate Table silver.crm_cust_info;
	Print '>> Inserting Data Into: silver.erp_loc_a101';

		Insert Into silver.erp_loc_a101(
				cid,
				cntry)
		Select
			Replace(Cid, '-', '') cid,
			Case When Trim(cntry) = 'DE' Then 'Germany'
				 When Trim(cntry) In ('US', 'USA') Then 'United States'
				 When Trim(cntry) = '' Or cntry Is Null Then 'n/a'
				 Else Trim(cntry)
			End As cntry
		From bronze.erp_loc_a101;
	Set @end_time = GetDate();
	Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
	Print Replicate('-', 20);


		--loading the erp_px_Cat_g1v2 
	Print '>> Truncating Table: silver.erp_px_cat_g1v2';
	Truncate Table silver.crm_cust_info;
	Print '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		Insert Into silver.erp_px_cat_g1v2(
			id, 
			cat, 
			subcat, 
			maintenance)
		Select
			id,
			cat,
			subcat,
			maintenance
	FROM bronze.erp_px_cat_g1v2;
	Set @end_time = GetDate();
	Print '>>> Load Duration: ' + Cast(DateDiff(second, @start_time, @end_time) As nvarchar) + ' seconds';
	Print Replicate('-', 20);

	Set @batch_end_time = GetDate();
		Print Replicate('=', 20);
		Print 'Loading Silver Layer is Completed';
        Print '   - Total Load Duration: ' + Cast(DateDiff(Second, @batch_start_time, @batch_end_time) AS nvarchar) + ' seconds';
		Print Replicate('=', 20);
		
	End Try
	Begin Catch
		Print Replicate('=', 20);
		Print 'Error Occured During Loading Bronze Layer'
		Print 'Error Message' + Error_Message();
		Print 'Error Message' + Cast (Error_Number() AS nvarchar);
		Print 'Error Message' + Cast (Error_state() AS nvarchar);
		Print Replicate('=', 20);
	End Catch

End
