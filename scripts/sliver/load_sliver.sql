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
   CALL silver.load_silver;
===============================================================================
*/


DROP PROCEDURE IF EXISTS sliver.load_sliver;
CREATE PROCEDURE sliver.load_sliver()

BEGIN 
    DECLARE v_error_message TEXT;
    DECLARE v_error_number INT;
    DECLARE v_sqlstate CHAR(5);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Get error details
        GET DIAGNOSTICS CONDITION 1
            v_error_message = MESSAGE_TEXT,
            v_error_number = MYSQL_ERRNO,
            v_sqlstate = RETURNED_SQLSTATE;

        -- Print messages
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURED DURING LOADING BRONZE LAYER' AS msg;
        SELECT CONCAT('Error Message: ', v_error_message) AS msg;
        SELECT CONCAT('Error Number: ', v_error_number) AS msg;
        SELECT CONCAT('Error State: ', v_sqlstate) AS msg;
        SELECT '==========================================' AS msg;
    END;


SET @batch_start_time = NOW();
SELECT '==========================================' AS msg;
SELECT 'Load Data into Sliver Layer' AS msg;
SELECT '==========================================' AS msg;
SELECT '==========================================' AS msg;
SELECT 'Load Data into CRM' AS msg;
SELECT '==========================================' AS msg;
SET @start_time = NOW();	
SELECT 'Truncate crm_cust_info' AS msg;
TRUNCATE TABLE sliver.crm_cust_info;
SELECT 'Insert Data into crm_cust_info' AS msg;
INSERT INTO sliver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,

CASE UPPER(TRIM(cst_marital_status))
	WHEN 'M' THEN 'Married'
	WHEN 'S' THEN 'Single'
	ELSE 'Unknown'
END cst_marital_status,

CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'Unknown'
END cst_gndr,
cst_create_date 
FROM (
SELECT 
*, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL)cci WHERE flag_last = 1;
SET @end_time = NOW();
SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SELECT 'Truncate crm_prd_info' AS msg;
SET @start_time = NOW();	
TRUNCATE TABLE sliver.crm_prd_info;
SELECT 'Insert into crm_prd_info' AS msg;
INSERT INTO sliver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
IFNULL(prd_cost, 0) AS prd_cost,
CASE  UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'S' THEN 'Other Sale'
	WHEN 'R' THEN 'Road'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
END prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1' DAY AS prd_end_dt
FROM bronze.crm_prd_info;
SET @end_time = NOW();

SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SELECT 'Truncate crm_sales_details' AS msg;
SET @start_time = NOW();	
TRUNCATE TABLE sliver.crm_sales_details;
SELECT 'Insert into crm_sales_details' AS msg;
INSERT INTO sliver.crm_sales_details 
(
sls_ord_num,
sls_prd_key,
sls_cus_id,
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
sls_cus_id,
CASE 
	WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 
	THEN NULL
ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
END AS sls_order_dt,
CASE 
	WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
END AS sls_ship_dt,
CASE 
	WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
END AS sls_due_dt, 
CASE 
	WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END sls_sales,
sls_quantity,
CASE  
	WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price
END sls_price 
FROM bronze.crm_sales_details;
SET @end_time = NOW();

SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SELECT '==========================================' AS msg;
SELECT 'Load Data into ERP' AS msg;
SELECT '==========================================' AS msg;

SELECT 'Truncate erp_cust_az12' AS msg;
SET @start_time = NOW();
TRUNCATE TABLE sliver.erp_cust_az12; 
SELECT 'Insert Data into erp_cust_az12' AS msg;
INSERT INTO sliver.erp_cust_az12(
cid,
bdate,
gen
)

SELECT
CASE 
	WHEN cid LIKE 'NAS%'  -- Remove 'NAS' prefix if present
	THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid	
END cid,
CASE
	WHEN bdate > CURDATE() -- set future date to NULL
	THEN NULL 
ELSE bdate
END bdate,
CASE
	WHEN  UPPER(TRIM(gen)) IN ('M','Male')
	THEN 'Male'
	WHEN  UPPER(TRIM(gen)) IN ('F','Female')
	THEN 'Female'
ELSE 'Unknown'
END gen -- Normalize gender value
FROM bronze.erp_cust_az12;
SET @end_time = NOW();
SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SELECT 'Truncate erp_loc_a101' AS msg;
SET @start_time = NOW();
SELECT 'Insert Data into erp_loc_a101' AS msg;
TRUNCATE TABLE sliver.erp_loc_a101;
INSERT INTO sliver.erp_loc_a101(
cid,
cntry
)
SELECT DISTINCT
REPLACE(cid,'-','') AS cid,
CASE 
	WHEN TRIM(cntry) IN ('US','USA')
	THEN 'United States'
	WHEN  TRIM(cntry) = 'DE'
	THEN 'Germany'
	WHEN  TRIM(cntry) = '' OR cntry IS NULL 
	THEN 'Unknown'
ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101;
SET @end_time = NOW();
SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SELECT 'Truncate erp_px_cat_g1v2' AS msg;
SET @start_time = NOW();
TRUNCATE TABLE sliver.erp_px_cat_g1v2;
SELECT 'Insert Data into erp_px_cat_g1v2' AS msg;
INSERT INTO sliver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)
SELECT  
id,
cat,
subcat,
maintenance 
FROM bronze.erp_px_cat_g1v2;
SET @end_time = NOW();
SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;

SET @batch_end_time = NOW();
SELECT CONCAT(
    '>> Load Duration: ',
    TIMESTAMPDIFF(SECOND, @batch_start_time, @batch_end_time),
    ' seconds'
) AS message;
END

CALL sliver.load_sliver;
