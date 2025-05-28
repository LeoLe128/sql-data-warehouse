/*
=================================================================
Quality Checks
=================================================================
Script Purpose:
	This script performes various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schemas. It includes checks for:
		- Null and duplicates primary keys
		- Unwanted spaces in string fields
		- Data standardization and consistency in category fields
		- Invalid date ranges and orders
		- Data consistency among fields

	Usage notes:
		- Run these checks after loading data from bronze layer to silver layer
		- Investigate and resolve any discrepancy found during the checks.
=================================================================
*/

--=================================================================
--Check silver.crm_cust_info
--Check for null or duplicates in the primary key
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

--Check for unwanted spaces
SELECT 
	cst_firstname,
	cst_lastname
FROM bronze.crm_cust_info;

SELECT 
	cst_firstname,
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname LIKE ' %' OR cst_lastname LIKE ' %';

--check for the categorical data
SELECT DISTINCT 
	cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT 
	cst_marital_status
FROM bronze.crm_cust_info

--not align with the rule: only store meaningful values 
--haven't established this rule yet

--Check quality of silver table data
--Check for null or duplicates in the primary key
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces
SELECT 
	cst_firstname,
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname LIKE ' %' OR cst_lastname LIKE ' %';

--check for the categorical data
SELECT DISTINCT 
	cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT 
	cst_marital_status
FROM silver.crm_cust_info

-- CRM.PRD_INFO
SELECT * FROM bronze.crm_prd_info;
--check for null or duplicate primary key
SELECT 
	prd_id
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1 OR prd_id IS NULL;
-- no null or duplicate value

--check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm LIKE ' %'
-- OKAY

--check for null or negative value in cost column
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

--check for categorical value in the line colummn
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--check for datetime 
SELECT * FROM silver.crm_prd_info

--CRM_SALES_DETAILS
--Check for invalid dates
--negative values
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) !=8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101 -- 19 invalid values

SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) !=8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101 --okay

--check the valid order of dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt NOT BETWEEN sls_order_dt AND sls_due_dt --okay

--check sales data
-- Expect: sales/revenue = quantity*price | No 0 or negative values | No NULL values
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE  
sls_sales != (sls_quantity*sls_price)
OR sls_sales IS NULL
OR sls_price IS NULL
OR sls_quantity IS NULL
OR sls_sales <= 0
OR sls_price <= 0
OR sls_quantity <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT 
	sls_sales AS old_sls_sales,
	sls_quantity AS old_sls_quantity,
	sls_price AS old_sls_price,
	CASE	
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price) THEN ABS(sls_quantity * sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales/sls_quantity)
		WHEN sls_price < 0 THEN ABS(sls_price)
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE  
sls_sales != (sls_quantity*sls_price)
OR sls_sales IS NULL
OR sls_price IS NULL
OR sls_quantity IS NULL
OR sls_sales <= 0
OR sls_price <= 0
OR sls_quantity <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--check erp cust table
--check valid cid
--expect: no null | no duplicate
SELECT cid, COUNT(cid)
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(cid) > 1 OR cid IS NULL 

--check if cid is usable to link to cust_info table
SELECT * FROM bronze.erp_cust_az12;
SELECT cst_id, cst_key FROM silver.crm_cust_info
--old data has NAS prefic in the cid

SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
FROM bronze.erp_cust_az12

-- check out-of-range or null bdates
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate IS NULL 
OR bdate < '1925-01-01' 
OR bdate > GETDATE()

--check gender data standardization and consistency
SELECT DISTINCT gen,
	CASE 
		WHEN UPPER(gen) = 'F' THEN 'Female'
		WHEN UPPER(gen) = 'M' THEN 'Male'
		WHEN gen IS NULL OR gen = '' THEN 'N/A'
		ELSE gen
	END AS gen
FROM bronze.erp_cust_az12

--ERP_LOC_A101
SELECT * FROM bronze.erp_loc_a101
--check null / duplicate cid
SELECT cid, COUNT(cid)
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(cid) > 1 

-- check if cid is compatible
SELECT cid FROM bronze.erp_loc_a101
SELECT cst_key FROM silver.crm_cust_info
/* have '-' in the middle of cid*/
SELECT 
	REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101

--check data standardization and consistency in cntry column
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT
	cntry,
	CASE	
		WHEN UPPER(cntry) = 'DE' THEN 'Germany'
		WHEN UPPER(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101

--erp_px_cat_g1v2
--check valid id
/*Expect: 
- Compatible with cat_id of crm_prd_info
- No null, no duplicate*/
SELECT id, COUNT(id) 
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(id) > 1 AND id IS NULL

--check unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat LIKE ' %'
OR subcat LIKE ' %'  
OR maintenance LIKE ' %'  

--check data standardization and consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2 
 
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2 

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2 
