#Data Cleaning Project

# 1 Removing Duplicates 
# 2 Standardizing the Data 
# 3 Null or Blank values
# 4 Removing any columns 

select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert into layoffs_staging
select* 
from layoffs;

select *
from layoffs_staging;

# we will now use window function with row number() to 

select *,
Row_Number() over(partition by company, location, industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
from layoffs_staging;

# why we directly did not do where row_num>1 in above statement and delete it is because above statement is a quey, it doesnt contain the column row_num 
# Therefore we need to create another table with row_num a s column and then delete dumplicates from it. 
with layoffs_CTE as
(
select *,
Row_Number() over(partition by company, location, industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select*
from layoffs_CTE
where row_num>1;

select *
from layoffs_staging
where company='Casper';

# now we want to delete the rows where row_num>1 but we cannot do this delete(which is an update command) on a cte 
# therefore we will make a new table called layoffs_staging2 where we will add another column to it called row_num and delete all  row with row_num>2. 

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

insert into layoffs_staging2
select*,Row_Number() over(partition by company, location, industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select * 
from layoffs_staging2;

delete
from layoffs_staging2
where row_num>1;

##--2] Standardizing the data 

select company
from layoffs_staging2;

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2              # company column fixed
set company=trim(company);

select distinct(industry) 
from layoffs_staging2
order by 1; 

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2  #industry column fixed 
set industry='Crypto'
where industry like 'Crypto%';

select *
from layoffs_staging2
where country like 'United States';

select distinct country, trim(trailing '.' from country) as new_country
from layoffs_staging2
order by 1 desc;

update layoffs_staging2
set country= trim(trailing '.' from country) 
where country like 'United States%';

#NOW COMING TO THE DATE COLUMN, THE DATE COLUMN IS IN TEXT DATATYPE/FORMAT, WE NEED TO CONVERT IT TO DATE FORMAT IF WE WANNA TO TIME SERIES FORECASTING/ ANALYSIS 

SELECT `date`			
from layoffs_staging2;

Update layoffs_staging2
set `date`=str_to_date(`date` , '%m/%d/%Y');

#By doing this the date value were converted to date format from text, but the column is still in text format

alter table layoffs_staging2
modify column `date` date;

#-----------------------------------------------------------
# 3] NULL OR BLANK VALUES

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry=null
where industry='';

select *
from layoffs_staging2
where industry is null 
or industry='';

select *
from layoffs_staging2
where company like 'Bally%';

select t1.industry,t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company=t2.company
where (t1.industry is null or t1.industry ='')
and t2.industry is not null;

update layoffs_staging2 as t1
join layoffs_staging as t2
	on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

select * 
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

# drop the row_num column

alter table layoffs_staging2
drop column row_num;
