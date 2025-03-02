select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null or blank values
-- 4. Remove any columns

-- Create a duplicate file first so that in event anything goes wrong, raw data still remains
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert into layoffs_staging
select *
from layoffs;

-- Check for duplicates


with duplicate_check as (
select *,
row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as dupli_check
from layoffs_staging
)

# if there are no duplicates, everything in dupli_check will be 1 as we group/partition by all columns
select *
from duplicate_check
where dupli_check > 1;

# in microsoft sql, apparantly we can just delete the rows using the same set of codes above changing select * to delete. 
# but in mysql, we need to create a new table with the dupli check column and delete the rows from there

# Create empty table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `dupli_check` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

# Append all the values into the table
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as dupli_check
from layoffs_staging;

# Delete the duplicate rows
delete
from layoffs_staging2
where dupli_check > 1;



-- Standardizing Data

# Check to see the removal white spaces
select distinct company, trim(company)
from layoffs_staging2;

# Updating dataset with new data
update layoffs_staging2
set company = trim(company);


select distinct industry
from layoffs_staging2
order by 1;            # 1 means the first column in select, therefore it can mean order by industry

# There were multiple things with crypto, crypto currency, cryptocurrency
select *
from layoffs_staging2
where industry like 'Crypto%';

# Update to all just Crypto
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

# Update United States. to just United States
update layoffs_staging2
set country = 'United States'
where country like 'United States%';

# Need to change date column as it is in text to date format
select `date`,
str_to_date(`date`, '%m/%d/%Y')  # str_to_date(column, format)
from layoffs_staging2;
 
# Update date column with the correct format
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

# Next we need to change the data type in the table from text to date
# Never do this on original dataset, only staging
alter table layoffs_staging2
modify column `date` date;


-- Dealing with null or blank values

# Do a quick check of each column to see where are the null values
select distinct industry
from layoffs_staging2
order by 1;

# Identified columns with missing values: industry, total_laid_off, percentage_laid_off, date, stage, funds_raised_millions

# 1. we try and populate the date we can
# Check industry first
select * 
from layoffs_staging2
where industry is null
or industry = '';

# Check whether we can get the industry from another row with the same company
select *
from layoffs_staging2
where company = "Airbnb";

# Update the blank values with null
update layoffs_staging2
set industry = null
where industry = '';

# Match all the empty industry to filled industry of the same company
select t1.company, t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

# Update all the empty industry with a populated industry of the same company
update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


# 2. Next we delete data that is not helpful. Try not to delete too many

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- Drop any columns not needed

alter table layoffs_staging2
drop column dupli_check;


select *
from layoffs_staging2;
































































