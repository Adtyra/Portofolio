-- Creating Table 
-----------------------------------------------------------------------
CREATE TABLE tb_faskes_bpjs (
    id SERIAL PRIMARY KEY,
    NoLink VARCHAR,
    Provinsi TEXT,
    Kotakab INT,
    Link DATE,
    TipeFaskes VARCHAR,
	No VARCHAR,
	KodeFaskes VARCHAR,
	NamaFaskes VARCHAR,
	LatLongFaskes TEXT,
	AlamatFaskes TEXT,
	TelpFaskes VARCHAR null
);

-- Duplicating CSV File Data
-- Source https://www.kaggle.com/datasets/israhabibi/list-faskes-bpjs-indonesia
-----------------------------------
COPY tb_faskes_bpjs (NoLink, Provinsi, Kotakab, Link, TipeFaskes, No, KodeFaskes, NamaFaskes, LatLongFaskes, AlamatFaskes, TelpFaskes) 
FROM 'E:\Data Faskes BPJS 2019.csv' 
WITH (FORMAT csv, HEADER true);

-- Creating Materialized View For Hospital/Province
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW rumah_sakit_per_provinsi AS
 SELECT
     provinsi,
     tipefaskes,
     COUNT(*) AS total_rumah_sakit
  FROM
      tb_faskes_bpjs
  WHERE
      tipefaskes = 'Rumah Sakit'
  GROUP BY
      Provinsi, tipefaskes;

-- Creating Materialized View For Health Center/Province
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW puskesmas_provinsi AS
 SELECT
     Provinsi,
     tipefaskes,
     COUNT(*) AS total_puskesmas
  FROM
      tb_faskes_bpjs
  WHERE
      tipefaskes = 'Puskesmas'
  GROUP BY
      Provinsi, tipefaskes;

-- Creating Materialized View For Health Facility in East Java Pronvince
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW Faskes_Jatim AS
 SELECT
     Provinsi,
     TipeFaskes,
     COUNT(*) AS total_faskes
  FROM
      tb_faskes_bpjs
  WHERE
      Provinsi = 'Jawa Timur'
  GROUP BY
      Provinsi, TipeFaskes;
