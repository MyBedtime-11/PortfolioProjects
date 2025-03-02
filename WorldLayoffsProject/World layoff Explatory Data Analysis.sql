-- Explatory Data Analysis

SELECT *
FROM layoffs_staging2;

# Total amount of layoffs by company
SELECT company, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY sum_laid_off desc;

# Date range: Start and End
SELECT MIN(date), MAX(date)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
ORDER BY date;


# Total amount of layoffs
SELECT SUM(total_laid_off)
FROM layoffs_staging2;


# Layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# Layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# No. of layoffs by year
SELECT YEAR(`date`),  SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

# No. of layoffs by stages, ordered by laid offs
SELECT stage,  SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

# Order by earliest month to latest month and we will do a rolling total as well
WITH month_rolling AS 
(
SELECT SUBSTRING(`date`, 1, 7) as `month`, sum(total_laid_off) as sum_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY 1
)

SELECT *,
SUM(sum_laid_off) OVER(ORDER BY `month`) as Rolling_total
FROM month_rolling;


-- Top 5 compaines with the highest laid offs in each year

# CTE ranks laid off from highest to lowest by each year
WITH company_year(company, `year`, sum_laid_off) AS 
(
SELECT  company, YEAR(`date`) , SUM(total_laid_off) 
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, YEAR(`date`)
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY 2 ASC, 3 DESC
), 

# Using dense_rank, we give a ranking to each of these company each year
# We order the dataset by rankings to get highest laid offs by each company in a year to the lowest
 ranked AS (
SELECT *,
DENSE_RANK() OVER(PARTITION BY `year` ORDER BY sum_laid_off DESC) as `rank`
FROM company_year
ORDER BY `rank`
)

# Getting top 5 companies with the highest laid off each year
SELECT * 
FROM ranked
WHERE `rank` <= 5
ORDER BY year;


-- Find out which industry has the highest number and of company going under and which industry has the highest percentage

# Number of compaines that gone under in each industry
WITH count_gone_under AS
(
SELECT industry, count(percentage_laid_off) as companies_gone_under
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY industry
ORDER BY count(percentage_laid_off) DESC
), 

# Number of compaines in each industry
count_per_industry AS
(
SELECT industry, COUNT(company) as count_in_industry
from layoffs_staging2
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY count_in_industry DESC
)

# Find the percentage of companies that went under in each industry
SELECT c1.industry,
c2.count_in_industry,
c1.companies_gone_under,
c1.companies_gone_under / c2.count_in_industry * 100 as percentage_under
FROM count_gone_under as c1
JOIN count_per_industry as c2
	ON c1.industry = c2.industry
ORDER BY percentage_under DESC
; 


-- Funds raised by compaines in each industry
# 1. Total raised by each company regardless of date and the company that raised the most in each industry
WITH total_raised AS 
(
SELECT company, industry, SUM(funds_raised_millions) as funds_raised_millions2
FROM layoffs_staging2
WHERE industry IS NOT NULL
GROUP BY company, industry
ORDER BY industry
), 

# Total raised by each company regardless of date
rolling_total as 
(
SELECT *,
SUM(funds_raised_millions2) OVER(PARTITION BY industry) AS rolling_funds_raised,
DENSE_RANK() OVER(PARTITION BY industry ORDER BY funds_raised_millions2 DESC) as `rank`
FROM total_raised
)

# Company that raised the most in each industry
SELECT *
FROM rolling_total
where `rank` = 1;




# 2. Total raised by each company separated by the date it is raised
SELECT company, industry, funds_raised_millions, `date`,
SUM(funds_raised_millions) OVER(PARTITION BY industry ORDER BY company ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rolling_funds_raised
FROM layoffs_staging2
WHERE industry IS NOT NULL;

-- 
SELECT country, COUNT(company)
FROM layoffs_staging2
GROUP BY country;

SELECT *
FROM layoffs_staging2;































