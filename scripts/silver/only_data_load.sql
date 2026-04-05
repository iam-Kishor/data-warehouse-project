PRINT '>> Starting Silver Layer Load Process <<'
PRINT ' '
PRINT '>> Truncating Table: silver.crm_cust_info to prepare for fresh load...'
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Loading data into silver.crm_cust_info...'

INSERT INTO silver.crm_cust_info(
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
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
		WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A'
	END AS
	cst_marital_status,
	CASE 
		WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
		WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'N/A'
	END AS
	cst_gndr,
	cst_create_date
FROM
(
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
	FROM BRONZE.crm_cust_info
	WHERE cst_id is not null
	)t WHERE flag = 1

PRINT ' '
PRINT '>> Truncating Table: silver.crm_prd_info to prepare for fresh load...'
TRUNCATE TABLE silver.crm_prd_info
PRINT '>> Loading data into silver.crm_prd_info...'
INSERT INTO silver.crm_prd_info(
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
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- Extracting Cat ID from prd_key
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extracting only prd_key from prd_key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS
	prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

PRINT ' '
PRINT '>> Truncating Table: silver.crm_sales_details to prepare for fresh load...'
TRUNCATE TABLE silver.crm_sales_details
PRINT '>> Loading data into silver.crm_sales_details...'
INSERT INTO silver.crm_sales_details(
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
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM BRONZE.crm_sales_details

PRINT ' '
PRINT '>> Truncating Table: silver.erp_cust_az12 to prepare for fresh load...'
TRUNCATE TABLE silver.erp_cust_az12
PRINT '>> Loading data into silver.erp_cust_az12...'
INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
	SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- NAS RECORDS SHOULD BE CLEANED TO REMOVE 'NAS' PREFIX
		ELSE cid 
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() OR bdate < '1900-01-01' THEN NULL -- INVALID BIRTHDATES SHOULD BE HANDLED (FUTURE AS NULL)
		ELSE bdate 
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		ELSE 'N/A'
	END AS gen
FROM bronze.erp_cust_az12

PRINT ' '
PRINT '>> Truncating Table: silver.erp_loc_a101 to prepare for fresh load...'
TRUNCATE TABLE silver.erp_loc_a101
PRINT '>> Loading data into silver.erp_loc_a101...'
INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
	SELECT
	REPLACE(cid,'-','') AS cid,
	CASE 
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry) -- OTHER COUNTRIES SHOULD BE KEPT AS IS, JUST TRIMMED
	END AS
	cntry
FROM bronze.erp_loc_a101

PRINT ' '
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2 to prepare for fresh load...'
TRUNCATE TABLE silver.erp_px_cat_g1v2
PRINT '>> Loading data into silver.erp_px_cat_g1v2...'
INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintainence
)
	SELECT
	id,
	cat,
	subcat,
	maintainence
FROM bronze.erp_px_cat_g1v2
