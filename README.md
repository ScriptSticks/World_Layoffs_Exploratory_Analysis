## Previewing the data
SELECT * FROM layoffs_staging;
![preview](https://github.com/user-attachments/assets/d4f814f7-0a95-4ca5-b130-f9299e8fe710)

#Exploratory Data Analysis: Exploring the data to find trends and patterns

## Checking if companies laid off mulitple dates to properly identify if layoffs happened only once or mulitple times
WITH Check_Multi_Layoff AS (SELECT company, ROW_NUMBER() OVER(PARTITION BY company, industry, location) count_multiples FROM (SELECT company, location, industry, date FROM layoffs_staging
GROUP BY company, location, industry, date) company_and_location)
SELECT * FROM Check_Multi_Layoff
WHERE count_multiples > 1;
![multiple_layoffs](https://github.com/user-attachments/assets/201a36c1-04eb-4a2f-bf6b-1e73d6e89a69)


## Checking the start and end date of the data
SELECT  MIN(`date`) start_date, MAX(`date`) end_date
FROM layoffs_staging;
![dates](https://github.com/user-attachments/assets/0e11c3ae-ad5f-4c2b-b7d2-de0ca224c945)

## Checking the maximum total layoff in a single day and maximum percentage layoff
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging
;
![max_tol_percent](https://github.com/user-attachments/assets/274cdc6d-4fde-4a5b-861d-902e84b3962a)

## Checking companies that laid off 100% of their staffs ordering by amount raised in descending order
SELECT * FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
![100_percent_layoff](https://github.com/user-attachments/assets/31408a09-32a4-490d-add5-76598d315b6f)

## Checking the highest layoff of each company by location (for companies in multiple locations)
SELECT company, location, MAX(total_laid_off) max_layoff FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company, location
ORDER BY 1 ASC, 3 DESC;
![locs](https://github.com/user-attachments/assets/b1201eeb-cdd7-472d-a287-4fdfa8f185a5)

## Aggregating the total layoff by each company.
SELECT company, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;
![offs](https://github.com/user-attachments/assets/6f581339-1e84-4b89-8008-5be4a412e8fe)

## Aggregating the total layoff by each industry.
SELECT industry, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;
![indus](https://github.com/user-attachments/assets/5c2a9f23-e377-4a9c-b7d1-4341d1b0a082)

## Calculating the total layoff for each year
SELECT YEAR(`date`) years, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY years
ORDER BY 1 DESC;
![years](https://github.com/user-attachments/assets/73ec9e0a-3670-4aed-99e1-8153ea31257b)

## Calculating the categories of stages that did the most layoff
SELECT stage, SUM(total_laid_off) sum_layoffs FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;
![stages](https://github.com/user-attachments/assets/df83ec2a-7641-4661-891b-81835e889eb9)

## Running total of total_layoffs paritioned by each company
SELECT *,
SUM(total_laid_off) OVER( ORDER BY company ) sum_layoffs
FROM layoffs_staging
WHERE `date` IS NOT NULL;
![run_tot](https://github.com/user-attachments/assets/fff05bb2-2eeb-4c6a-aaa5-aecd5accefd6)

## Rolling total of total_layoffs paritioned by the months in each year
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
![roll_tot](https://github.com/user-attachments/assets/c627e647-3f72-4364-b344-936aef563f46)

## Top 5 Highest Layoff Per Year by Companies
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
![year_rank](https://github.com/user-attachments/assets/e4bcefdf-a1b0-49f6-90bc-c436531f75b1)

## Top 5 Highest Layoff Per Year by Industries
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
![year_rank_industry](https://github.com/user-attachments/assets/4e64070f-c40d-4c95-a731-829603bc6f1b)

## Identified companies in mulitple location to check the location with the highest number of total layoffs
## Step 1
SELECT company, location,industry, SUM(total_laid_off) sum_total_layoff
FROM layoffs_staging
GROUP BY company, location, industry
ORDER BY 1
;

## Step 2
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

## Step 3
SELECT * FROM (
		SELECT company, location,industry, SUM(total_laid_off) sum_total_layoff
		FROM layoffs_staging
		GROUP BY company, location, industry) tab
	WHERE company IN (SELECT * FROM company_multiple_location)
    ORDER BY 1, 4 DESC;
    ![complex](https://github.com/user-attachments/assets/c8d33938-6276-4b95-a396-a90e22ae2b74)
