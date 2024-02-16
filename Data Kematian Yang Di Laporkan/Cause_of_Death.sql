-- Creating Table 
-----------------------------------------------------------------------
CREATE TABLE tb_penyebab_kematian (
    id SERIAL PRIMARY KEY,
    Cause VARCHAR,
    Type TEXT,
    Year VARCHAR,
    Data_Redundancy INT,
	Total_Deaths INT,
	Source VARCHAR,
	Page_at_Source VARCHAR,
	Source_URL TEXT
); 
-- Duplicating CSV File Data
-- Source https://www.kaggle.com/datasets/hendratno/cause-of-death-in-indonesia
-----------------------------------
COPY tb_penyebab_kematian (Cause,Type,Year,Data_Redundancy,Total_Deaths,Source,Page_at_Source,Source_URL) 
FROM 'E:\Penyebab Kematian di Indonesia yang Dilaporkan - Clean.csv' 
WITH (FORMAT csv, HEADER true);

-- Creating Materialized View For Cause of Death
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW tipe_penyebab AS
SELECT
    cause,
    SUM(total_deaths) AS total_deaths
FROM
    tb_penyebab_kematian
GROUP BY
    cause
ORDER BY
    cause;
-- Creating Materialized View For Number of Death per Year
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW total_death_Year AS
SELECT
    year,
    SUM(total_deaths) AS total_deaths
FROM
    tb_penyebab_kematian
GROUP BY
    year
ORDER BY
    year;
    
-- Creating Materialized View For Death of AIDS by Year
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW aids_deaths_by_year AS
SELECT
    year,
    SUM(total_deaths) AS total_deaths
FROM
    tb_penyebab_kematian
WHERE
    cause = 'AIDS'
GROUP BY
    year
ORDER BY
    year ASC;
