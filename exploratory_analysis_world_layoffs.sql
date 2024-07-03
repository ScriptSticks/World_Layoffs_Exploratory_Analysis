SELECT * FROM layoffs_staging;

#Exploratory Data Analysis: Exploring the data to find trends and patterns
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging
;

#Checking companies with the highest percentage of layoffs
SELECT * FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

#Checking the start and end date of the data
SELECT  MIN(`date`) start_date, MAX(`date`) end_date
FROM layoffs_staging;

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

-- Rolling total of total_layoffs paritioned by each company
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
	FROM company_year_rank
	WHERE _year IS NOT NULL)
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

SELECT * FROM layoffs_staging;