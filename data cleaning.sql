-- data cleaning

-- 1-remove dublicates
-- 2-standralize the data 
-- 3-null values or blamk
-- 4-remove any columns


-- 1-remove dublicates
create table layoffs_staging
like layoffs ;

select * from layoffs_staging;
insert layoffs_staging
select * from layoffs;

with layof as ( select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging)
select * from layof where row_num > 1;
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

select* from layoffs_staging2;

insert into layoffs_staging2 
 select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging;

delete from layoffs_staging2
where row_num >1
;
SET SQL_SAFE_UPDATES = 0;
select*from layoffs_staging2 where row_num>1 ;

-- 2-standardizing data

select distinct trim(company) from layoffs_staging2;
update  layoffs_staging2
set
 company =  trim(company);
select distinct country
 from layoffs_staging2 order by 1;
 update layoffs_staging2 set country = trim(trailing '.' from country)
where country like 'United States%';
select distinct country  from layoffs_staging2 order by 1 ;
update layoffs_staging2 set country = 'United States' where country = 'us' ;
select `date`, str_to_date( `date`, '%m/%d/%Y')
 from layoffs_staging2;
 update layoffs_staging2 set `date`=  str_to_date( `date`, '%m/%d/%Y') ;
 
alter table layoffs_staging2
modify column `date` date;


-- 3-null values or blamk

select * from layoffs_staging2
where industry = "" or industry is null;
update layoffs_staging2 set 
industry = null where industry = "";
select t2.company,t2.location,t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company 
and t1.location=t2.location
where t1.industry is null and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company 
and t1.location=t2.location 
set t1.industry=t2.industry where t1.industry is null and t2.industry is not null;

-- 4-remove any columns

alter table layoffs_staging2
drop column row_num;
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null ;

delete 
	from layoffs_staging2
		where total_laid_off is null and percentage_laid_off is null ;
