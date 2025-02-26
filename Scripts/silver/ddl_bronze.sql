If Object_Id('silver.crm_cust_info', 'U') Is Not Null
    Drop Table silver.crm_cust_info;

Create Table silver.crm_cust_info (
    cst_id             int,
    cst_key            nvarchar(50),
    cst_firstname      nvarchar(50),
    cst_lastname       nvarchar(50),
    cst_marital_status nvarchar(50),
    cst_gndr           nvarchar(50),
    cst_create_date    Date,
    dwh_create_date    DateTime2 Default GetDate()
);


If Object_Id('silver.crm_prd_info', 'U') Is Not Null
    Drop Table silver.crm_prd_info;


CREATE TABLE silver.crm_prd_info (
    prd_id          int,
    cat_id          nvarchar(50),
    prd_key         nvarchar(50),
    prd_nm          nvarchar(50),
    prd_cost        int,
    prd_line        nvarchar(50),
    prd_start_dt    date,
    prd_end_dt      date,
    dwh_create_date  DateTime2 Default GetDate()
);


If Object_Id('silver.crm_sales_details', 'U') Is Not Null
    Drop Table silver.crm_sales_details;


Create Table silver.crm_sales_details (
    sls_ord_num     nvarchar(50),
    sls_prd_key     nvarchar(50),
    sls_cust_id     int,
    sls_order_dt    date,
    sls_ship_dt     date,
    sls_due_dt      date,
    sls_sales       int,
    sls_quantity    int,
    sls_price       int,
    dwh_create_date  DateTime2 Default GetDate()
);


If Object_Id('silver.erp_loc_a101', 'U') Is Not Null
    Drop Table silver.erp_loc_a101;


Create Table silver.erp_loc_a101 (
    cid             nvarchar(50),
    cntry           nvarchar(50),
    dwh_create_date  DateTime2 Default GetDate()
);


If Object_Id('silver.erp_cust_az12', 'U') Is Not Null
    Drop Table silver.erp_cust_az12;


Create Table silver.erp_cust_az12 (
    cid             nvarchar(50),
    bdate           DATE,
    gen             nvarchar(50),
    dwh_create_date  DateTime2 Default GetDate()
);


If Object_Id('silver.erp_px_cat_g1v2', 'U') Is Not Null
    Drop Table silver.erp_px_cat_g1v2;


Create Table silver.erp_px_cat_g1v2 (
    id              nvarchar(50),
    cat             nvarchar(50),
    subcat          nvarchar(50),
    maintenance     nvarchar(50),
    dwh_create_date Datetime2 Default GetDate()
);

