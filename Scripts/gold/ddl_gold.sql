/*
===============================================================================
DDL Script: Create Gold Schema Tables
===============================================================================
DescriptiOn:
    ThIs script sets up the Views for the Gold layer in the data warehouse.
    - ThIs Gold Layer represents the final dimensiOn adn fact tables bAsed On the Star Schema. 

Usage:
    - These Gold table Views can be queried directly for analytics and reporting
===============================================================================
*/

If OBJECT_ID('Gold.dim_customers', 'V') Is Not Null
    Drop View Gold.dim_customers;
Go

Create View Gold.dim_customers As
Select
    Row_Number() OVER (Order BY cst_id) As customer_key, -- Surrogate key
    ci.cst_id                          As customer_id,
    ci.cst_key                         As customer_number,
    ci.cst_firstname                   As first_name,
    ci.cst_lAstname                    As lAst_name,
    la.cntry                           As country,
    ci.cst_marital_status              As marital_status,
    Case 
        When ci.cst_gndr != 'n/a' Then ci.cst_gndr -- CRM Is the primary source for gEnder
        Else COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    End                                As gEnder,
    ca.bdate                           As birthdate,
    ci.cst_Create_date                 As Create_date
From silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
    On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
    On ci.cst_key = la.cid;
Go

-- =============================================================================
-- Create DimensiOn: Gold.dim_products
-- =============================================================================
If OBJECT_ID('Gold.dim_products', 'V') Is Not Null
    Drop View Gold.dim_products;
Go

Create View Gold.dim_products As
Select
    Row_Number() OVER (Order BY pn.prd_start_dt, pn.prd_key) As product_key, -- Surrogate key
    pn.prd_id       As product_id,
    pn.prd_key      As product_number,
    pn.prd_nm       As product_name,
    pn.cat_id       As cateGory_id,
    pc.cat          As cateGory,
    pc.subcat       As subcateGory,
    pc.maintenance  As maintenance,
    pn.prd_cost     As cost,
    pn.prd_line     As product_line,
    pn.prd_start_dt As start_date
From silver.crm_prd_info pn
Left Join silver.erp_px_cat_g1v2 pc
    On pn.cat_id = pc.id
WHERE pn.prd_End_dt Is Null; -- Filter out all hIstorical data
Go

-- Creating Fact Table: Gold.fact_sales
If OBJECT_ID('Gold.fact_sales', 'V') Is Not Null
    Drop View Gold.fact_sales;
Go

Create View Gold.fact_sales As
Select
    sd.sls_ord_num  As order_number,
    pr.product_key  As product_key,
    cu.customer_key As customer_key,
    sd.sls_order_dt As order_date,
    sd.sls_ship_dt  As shipping_date,
    sd.sls_due_dt   As due_date,
    sd.sls_sales    As sales_amount,
    sd.sls_quantity As quantity,
    sd.sls_price    As price
From silver.crm_sales_details sd
Left Join Gold.dim_products pr
    On sd.sls_prd_key = pr.product_number
Left Join Gold.dim_customers cu
    On sd.sls_cust_id = cu.customer_id;
Go
