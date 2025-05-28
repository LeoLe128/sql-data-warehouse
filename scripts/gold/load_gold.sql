/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--create the dim_customers view
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
--create the dim_customers view
CREATE VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE 
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr --CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'N/A')--if the 1st object is NULL, use the 2nd one.
	END AS gender,
	ci.cst_marital_status AS marital_status,
	ca.bdate AS birthday,
	la.cntry AS country,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ca.cid = ci.cst_key
LEFT JOIN silver.erp_loc_a101 la
	ON la.cid = ci.cst_key
GO

--create the dim_products view
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key, --WHY??
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	pcg.cat AS cateogory,
	pcg.subcat AS sub_category,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date,
	pcg.maintenance
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 pcg
	ON pcg.id = pr.cat_id
WHERE pr.prd_end_dt IS NULL --filter out the historical data
GO 

--create fact_sales view
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sale_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
	ON dc.customer_id = sd.sls_cust_id
LEFT JOIN gold.dim_products dp
	ON dp.product_number = sd.sls_prd_key;
GO
