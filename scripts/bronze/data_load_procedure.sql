/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.batchload AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=========================================';
		PRINT 'LOADING BRONZE LAYER...';
		PRINT'=========================================';
		PRINT' ';
		
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'=========================================';
		PRINT'LOADING CRM TABLES...';
		PRINT'=========================================';
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_CRM\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- Lock the table during the bulk insert for better performance
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		TRUNCATE TABLE bronze.crm_prd_info;
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_CRM\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		TRUNCATE TABLE bronze.crm_sales_details;
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_CRM\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT' ';
		PRINT'=========================================';
		PRINT 'LOADINNG ERP TABLES...';
		PRINT'=========================================';
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_ERP\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		TRUNCATE TABLE bronze.erp_loc_a101;
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_loc_a101
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_ERP\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'K:\Projects\Git Repos\Data Warehouse Project\Datasets\Source_ERP\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT' ';
		PRINT'>>TIME TAKEN TO LOAD: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		SET @batch_end_time = GETDATE();

		PRINT' ';
		PRINT'=========================================';
		PRINT 'LOADING COMPLETED SUCCESSFULLY...';
		PRINT'=========================================';
		PRINT' ';
		PRINT'>>TIME TAKEN FOR BRONZE BATCH LOAD: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS';
	-- HERE UPDATEd AS BATCH LOAD INDICATING WE ARE USING TRUNCATE AND LOAD APPROACH
	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OBSERVED DURING BRONZE LAYER LOAD'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH
END
