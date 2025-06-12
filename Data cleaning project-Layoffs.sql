-- Data Cleaning project --

select*
from layoffs;

-- Steps in data cleaning --

# Remove duplicates

# Standardise the data- Check spellings

# Null or Blank values- Remove Nulls and fill in blank values where possible

# Remove any columns

-- Staging -- Creating a copy of the raw data


create table layoffs_staging
like layoffs;

select*
from layoffs_staging;

insert into layoffs_staging
select*
from layoffs;

select*
from layoffs_staging;

-- Removing duplicates-Creating row no.s. If the row no. is >= 2 then there are duplicates

select*,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions)as row_num
from layoffs_staging;

-- Then we create a CTE to remove duplicates--

with duplicate_cte as
(
select*,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions)as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;


-- Practice subquery method--

select*
from (select*,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions)as row_num
from layoffs_staging) as Dup_rn
where row_num > 1;                    # Subquery in WHERE statement


-- To verify --

select*
from layoffs_staging
where company = 'Casper';

-- Creating a copy of the staging table --

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select*
from layoffs_staging2;

insert into layoffs_staging2  # Inserting the data
select*,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions)as row_num
from layoffs_staging;

select*
from layoffs_staging2   
where row_num > 1;       # Identifying the duplicates

delete					# Deleting the duplicates
from layoffs_staging2   
where row_num > 1;

select*
from layoffs_staging2;

-- Standardising data --

select company, trim(company)  #trim to remove space before the company names
from layoffs_staging2;

update layoffs_staging2
set company = trim(company); # Updating in the actual table

select distinct industry
from layoffs_staging2
order by 1 ;				#Rearranging by alpha


update layoffs_staging2  		#Updating the industry name Crypto
set industry = 'Crypto'
where industry like 'Crypto%';  

select distinct location
from layoffs_staging2
order by 1;						# Everything is Ok so far

select distinct country
from layoffs_staging2
order by 1;

select distinct country, trim(Trailing '.' from country)
from layoffs_staging2
order by 1; 				#Correcting country name using trim

Update layoffs_staging2
set country = 'United States'
where country like 'United States%';		#Can be done as a permanent update

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y'); #m for month, Y for four digit year

select*
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

select*
from layoffs_staging2;

#Nulls and blank values

select*
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is  null; # to show the nulls in total_laid_off & percentage_laid_off

select*
from layoffs_staging2
where industry is null
or industry = '';				  # to show the blanks in industry

# we then set all the blanks to nulls

update layoffs_staging2
set industry = null
where industry = '';

select company, industry
from layoffs_staging2
where company = 'Airbnb';

# To populate the blanks, we identify if there any non blanks of the same company

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')  # to identify any non blanks of the same company
and t2.industry is not null;

# Update the nulls to proper industry name

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select*
from layoffs_staging2;

# We delete the rows with no data at all

delete
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is  null;

select*
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is  null;

#Then we drop the row_num column

alter table layoffs_staging2
drop row_num;

select*
from layoffs_staging2;












































