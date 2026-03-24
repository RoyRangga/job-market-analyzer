{{ config(
    materialized='table'
) }}

with clean_education as (
    SELECT
    link, 
    education
    FROM {{ source('internal_postgres_data', 'clean_education') }}
),

clean_major_tb as (
    SELECT 
    link, 
    major
    FROM {{ source('internal_postgres_data', 'clean_major_tb') }}
),

clean_job_skills as (
    SELECT
    link,
    hard_skills
    FROM {{ source('internal_postgres_data', 'clean_job_skills') }}
),

exp_tb_clean as (
    SELECT
    link,
    CASE 
        when min_exp like '%–%' then REPLACE(lower(min_exp), '–', '-')
        when min_exp like '%—%' then REPLACE(lower(min_exp), '—', '-')
        when min_exp like '%−%' then REPLACE(lower(min_exp), '−', '-')
        when min_exp like '% −%' then REPLACE(lower(min_exp), ' −', '-')
        when min_exp like '% −%' then REPLACE(lower(min_exp), ' −', '-')
        when min_exp like '% –%' then REPLACE(lower(min_exp), ' –', '-')
        when min_exp like '% -%' then REPLACE(lower(min_exp), ' -', '-')
        when min_exp like '%− %' then REPLACE(lower(min_exp), '− ', '-')
        when min_exp like '%− %' then REPLACE(lower(min_exp), '− ', '-')
        when min_exp like '%– %' then REPLACE(lower(min_exp), '– ', '-')
        when min_exp like '%- %' then REPLACE(lower(min_exp), '- ', '-')
        else lower(min_exp)
    END AS min_exp
    FROM {{ source('internal_postgres_data', 'exp_tb_clean') }}
),

remain_tb as (
    SELECT
    link,
    source_site, 
    location,
    CASE 
        when location like '%,%' then lower(trim(split_part(location, ',', 1)))
        else NULL
    END as city,
    CASE
        when location like '%,%' then lower(trim(split_part(location, ',', 2)) )
        else trim(location)
    END as province,
    role, 
    CASE
        when salary like '%per%' then trim(split_part(salary, 'per', 1))
        else salary
    END AS salary,
    CASE
        when salary like '%per%' then trim(split_part(salary, 'per', 2))
        else salary
    END AS PER,
    job_type, 
    company,
    position,
    specialization,
    industry,
    date_posted
    FROM {{ source('internal_postgres_data', 'remain_tb') }}
)

SELECT
    re.link as link,
    re.source_site, 
    re.location,
    re.city,
    TRIM(REPLACE(re.province, 'di', '')) as province,
    re.role, 
    re.salary,
    re.PER,
    re.job_type, 
    re.company,
    re.position,
    lower(specialization) as specialization,
    lower(industry) as industry,
    re.date_posted,
    lower(edu.education) as education,
    lower(major.major) as major,
    lower(skil.hard_skills) as hard_skills,
    REPLACE(
        REPLACE(
            lower(exp.min_exp), 'years', 'tahun'
        ), 'year', 'tahun'
    ) as min_exp
from remain_tb re 
left join clean_education edu on edu.link = re.link
left join clean_major_tb major on major.link = re.link
left join clean_job_skills skil on skil.link = re.link
left join exp_tb_clean exp on exp.link = re.link