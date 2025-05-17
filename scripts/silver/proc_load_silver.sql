/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

EXEC silver.load_silver
--Create stored procedures
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '------------------------------------------------';

		--load the silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncate table silver.crm_cust_info first'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Insert data into table silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname ,
			CASE 
				WHEN UPPER(cst_marital_status) = 'M' THEN 'Married' 
				WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
				ELSE 'N/A' 
			END AS cst_marital_status, --Standardize marital status to readable format
			CASE 
				WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
				WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
				ELSE 'N/A' 
			END AS cst_gndr, --Standardize gender to readable format
			cst_create_date
		FROM (
			SELECT *, 
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL) AS t
			WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';

		--load the silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncate table silver.crm_prd_info first'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Insert data into table silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_key,
			cat_id,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT 
			prd_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --extract product key
			REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,  --extract category id
			prd_nm,
			ISNULL(prd_cost,0)AS prd_cost,
			CASE UPPER(prd_line)
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END  AS	prd_line, --Map product line codes with descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';

		--load the silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncate table silver.crm_sales_details first'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Insert data into table silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
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
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt <=0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END AS sls_order_dt, 
			CASE 
				WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
			END AS sls_ship_dt, 
			CASE 
				WHEN sls_due_dt <=0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
			END AS sls_due_dt,
			CASE	
				WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price) THEN ABS(sls_quantity * sls_price)
				ELSE sls_sales
			END AS sls_sales, --Recalculate sales in case the original value is missing or invalid
			sls_quantity,
			CASE 
				WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales/sls_quantity)
				WHEN sls_price < 0 THEN ABS(sls_price)
			ELSE sls_price
			END AS sls_price --Derive price if the original value is invalid
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';
	
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '------------------------------------------------';

		--load the silver.erp_cust_az12
		PRINT '>> Truncate table silver.erp_cust_az12 first'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Insert data into table silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove 'NAS' prefix if present
				ELSE cid
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- set future bdate to NULL
			CASE 
				WHEN UPPER(gen) = 'F' THEN 'Female'
				WHEN UPPER(gen) = 'M' THEN 'Male'
				WHEN gen IS NULL OR gen = '' THEN 'N/A'
				ELSE gen
			END AS gen --standardize gender values and handle unknown cases
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';

		--load the silver.erp_loc_a101
		PRINT '>> Truncate table silver.erp_loc_a101 first'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Insert data into table silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE	
				WHEN UPPER(cntry) = 'DE' THEN 'Germany'
				WHEN UPPER(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
				ELSE cntry
			END AS cntry --standardize and handling missing country codes
		FROM bronze.erp_loc_a101	
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';

		--load the silver.erp_px_cat_g1v2
		PRINT '>> Truncate table silver.erp_px_cat_g1v2 first'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Insert data into table silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @end_time, @start_time) AS VARCHAR) + 'seconds.';
		PRINT '--------------';

		SET @batch_end_time = GETDATE()
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
		PRINT '>> Total oad duration: ' + CAST(DATEDIFF(SECOND, @batch_end_time, @batch_start_time) AS VARCHAR) + 'seconds.';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERRORS OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error message:' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR); --consideration for personal improvement
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
