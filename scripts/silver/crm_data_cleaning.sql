--EDA, just observing our data, noting any cleaning that needs to happen
--duplicates, data cleaning

-- so working with duplicates
-- which cst_id do u choose, ideally we go for the most recent 
--  so we want to rank each cst_id entry by the create_date


-- this query retrieves only the most  recent cst_ids created 
use DataWarehouse;
go 


select *
from (select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)t
where flag_last = 1  

--check for unwanted spaces
select cst_lastname
from bronze.crm_cust_info 
where cst_firstname != trim(cst_lastname);


insert into silver.crm_cust_info(
	cst_id, cst_key, cst_firstname, 
	 cst_lastname, 
	cst_marital_status,
	cst_gndr, 
	cst_create_date)

--standardiation and consistency 
select cst_id, cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname,
case when upper(cst_marital_status) = 'S' then 'Single'
	when upper(cst_marital_status) = 'M' then 'Married'
	else 'n/a' 
end cst_marital_status , 
case when upper(cst_gndr) = 'F' then 'Female'
	when upper(cst_gndr) = 'M' then 'Male'
	else 'n/a' 
end cst_gndr,
cst_create_date 
from (select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)t
where flag_last = 1   

select top 100 *
from silver.crm_cust_info

--silver.crm_prd_info
-- insert cleaned bronze layer data into silver layer
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select prd_id, prd_key, 
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm, 
	isnull(prd_cost, 0) as prd_cost, 
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'R' then 'Road'
		when upper(trim(prd_line)) = 'S' then 'Other Sales'
		when upper(trim(prd_line)) = 'T' then 'Touring'
		else 'n/a'
	end prd_line, 
	prd_start_dt , 
	--This query aims to fix the end date error
	-- the end date = the start date of the next record
	-- using lead() funct to access the startdate of the next row
	LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt ) -1 as prd_end_dt

from bronze.crm_prd_info


 
