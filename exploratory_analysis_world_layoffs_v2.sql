SELECT * FROM layoffs_staging;
#Exploratory Data Analysis: Exploring the data to find trends and patterns

#Checking if companies laid off mulitple dates to properly identify if layoffs happened only once or mulitple times
WITH Check_Multi_Layoff AS (SELECT company, ROW_NUMBER() OVER(PARTITION BY company, industry, location) count_multiples FROM (SELECT company, location, industry, date FROM layoffs_staging
GROUP BY company, location, industry, date) company_and_location)
SELECT * FROM Check_Multi_Layoff
WHERE count_multiples > 1;

#Checking the start and end date of the data
SELECT  MIN(`date`) start_date, MAX(`date`) end_date
FROM layoffs_staging;

#Checking the maximum total layoff in a single day and maximum percentage layoff
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging
;

#Checking companies that laid off 100% of their staffs ordering by amount raised in descending order
SELECT * FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Checking the highest layoff of each company by location (for companies in multiple locations)
SELECT company, location, MAX(total_laid_off) max_layoff FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company, location
ORDER BY 1 ASC, 3 DESC;

#Aggregating the total layoff by each company.
SELECT company, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

#Aggregating the total layoff by each industry.
SELECT industry, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

#Calculating the total layoff for each year
SELECT YEAR(`date`) years, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY years
ORDER BY 1 DESC;

#Calculating the categories of stages that did the most layoff
SELECT stage, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

-- Running total of total_layoffs paritioned by each company
SELECT *,
SUM(total_laid_off) OVER( ORDER BY company ) sum_layoffs
FROM layoffs_staging
WHERE `date` IS NOT NULL;

-- Rolling total of total_layoffs paritioned by the months in each year
WITH rolling_table AS (
SELECT SUBSTRING(`date`, 1, 7) `month`,
SUM(total_laid_off) sum_layoffs
FROM layoffs_staging
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1)
SELECT *,
SUM(sum_layoffs) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_table;

WITH company_year_rank AS (
	SELECT company, YEAR(`date`) _year,
	SUM(total_laid_off) total_layoff
	FROM layoffs_staging
	WHERE YEAR(`date`) IS NOT NULL
	GROUP BY company, YEAR(`date`) 
	ORDER BY 3 DESC),
	company_year_rank2 AS (
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY _year ORDER BY total_layoff DESC) rank_num
	FROM company_year_rank)
SELECT * FROM company_year_rank2
WHERE rank_num <= 5;

WITH industry_tab AS (
	SELECT industry, 
	YEAR(`date`) years,
	SUM(total_laid_off) total_layoff
	FROM layoffs_staging
	WHERE total_laid_off IS NOT NULL AND YEAR(`date`) IS NOT NULL
	GROUP BY industry, YEAR(`date`)
	ORDER BY 3 DESC
),
industry_tab2 AS (
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_layoff DESC) yearly_rank
	FROM industry_tab
)
SELECT * FROM industry_tab2
WHERE yearly_rank <= 5;

-- Identified companies in mulitple location to check the location with the highest number of total layoffs
-- Step 1
SELECT company, location,industry, SUM(total_laid_off) sum_total_layoff
FROM layoffs_staging
GROUP BY company, location, industry
ORDER BY 1
;

-- Step 2
CREATE TABLE company_multiple_location
WITH Multi_Appearing AS (	
	SELECT *, 
	ROW_NUMBER() OVER(PARTITION BY company) multiple
	FROM (
		SELECT company, location,industry, SUM(total_laid_off) sum_total_layoff
		FROM layoffs_staging
		GROUP BY company, location, industry
		ORDER BY 1
	) tier1
), Tab2 AS  (
SELECT * FROM Multi_Appearing
WHERE multiple = 2)
SELECT company FROM Tab2;

-- Step 3
SELECT * FROM (
		SELECT company, location,industry, SUM(total_laid_off) sum_total_layoff
		FROM layoffs_staging
		GROUP BY company, location, industry) tab
	WHERE company IN (SELECT * FROM company_multiple_location)
    ORDER BY 1, 4 DESC;
    
SELECT * FROM layoffs_staging;