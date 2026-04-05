-- Checks nulls in the cst_id column of the BRONZE.crm_cust_info table

SELECT * FROM BRONZE.crm_cust_info
WHERE cst_id IS NULL

-- Checks duplicates in the cst_id column of the BRONZE.crm_cust_info table

SELECT 
	cst_id,
	COUNT(cst_id) 
FROM BRONZE.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1

SELECT * FROM BRONZE.crm_cust_info
WHERE cst_id = 29449

-- Retrieves the latest record for each cst_id from the BRONZE.crm_cust_info table based on the cst_create_date column

SELECT 
* 
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
FROM BRONZE.crm_cust_info
)t WHERE flag = 1

-- Below query gives duplicated data

SELECT 
* 
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
FROM BRONZE.crm_cust_info
)t WHERE flag != 1

-- Checking extra spaces in firstname and lastname

SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname 
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- No Extra Spaces found for below columns

SELECT cst_gndr 
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

SELECT cst_marital_status 
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

-- Checking for distinct values to update using case statement

SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info
SELECT DISTINCT cst_marital_status from bronze.crm_cust_info

-- Quality check for Silver Layer

-- Below query checks duplicated data

SELECT 
* 
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
FROM silver.crm_cust_info
)t WHERE flag != 1

-- Checking extra spaces in firstname and lastname

SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname 
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- No Extra Spaces found for below columns

SELECT cst_gndr 
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

SELECT cst_marital_status 
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

SELECT DISTINCT cst_gndr FROM silver.crm_cust_info
SELECT DISTINCT cst_marital_status from silver.crm_cust_info

-- QUALITY CHECK FOR PRD_INFO TABLE

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

SELECT DISTINCT prd_line FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- Check for invalid dates

SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

-- Handling '0' by using NULL IF

SELECT  
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM BRONZE.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- CHECK FOR INVALID DATE ORDER
-- ORDER DATE NEEDS > SHIP DATE & ORDER DATE > DUE DATE

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- CHECK DATA INCONSISTENCY BETWEEN SALES, QUANTITY AND PRICE
-- >> SALES SHOULD BE EQUAL TO QUANTITY * PRICE
-- >> SALES, QUANTITY AND PRICE SHOULD NOT BE NULL

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity AS old_sls_quantity,
sls_price AS old_sls_price,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE 
	WHEN sls_price IS NULL OR sls_price <= 0 
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- CHECK FOR INVALID BIRTHDATES IN ERP_CUST_AZ12 TABLE

SELECT * FROM bronze.erp_cust_az12
WHERE bdate > GETDATE() OR bdate < '1900-01-01'

SELECT DISTINCT gen FROM bronze.erp_cust_az12

-- ERP LOCATION TABLE CHECK

SELECT DISTINCT cntry FROM bronze.erp_loc_a101
ORDER BY cntry

-- GOLD LAYER CHECKS

-- CUSTOMER DIMENSION CHECKS

-- CHECK DUPLICATES AFTER JOINS

SELECT cst_id, COUNT(*)	FROM
(SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON		ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON		ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1

-- CHECK FOR GENDER INCONSISTENCY BETWEEN CRM_CUST_INFO AND ERP_CUST_AZ12 TABLES

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen, 'n/a')
END AS final_gndr
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid
ORDER BY 1,2

-- PRODUCT DIMENSION CHECKS

-- CHECK FOR DUPLICATES

SELECT prd_id, COUNT(*) FROM
	(SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	prd_start_dt,
	PC.cat,
	PC.subcat,
	PC.maintainence
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON		pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL -- FILTER TO GET ONLY CURRENTLY ACTIVE PRODUCTS (WHICH IS NULL)
)t GROUP BY prd_id
HAVING COUNT(*) > 1

-- SALES FACT CHECKS

-- FOREIGN KEY INTEGRITY (DIMENSIONS)

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
