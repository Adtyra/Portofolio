-- Creating Table 
-----------------------------------------------------------------------
CREATE TABLE gojekreview (
    id SERIAL PRIMARY KEY,
    username VARCHAR,
    content TEXT,
    score INT,
    score_date DATE,
    appversion VARCHAR
);
-- Duplicating CSV File Data
-- Source https://www.kaggle.com/datasets/ucupsedaya/gojek-app-reviews-bahasa-indonesia
-----------------------------------
COPY gojekreview (username, content, score, score_date, appversion) 
FROM 'E:\gojekreview.csv' 
WITH (FORMAT csv, HEADER true);


-- Creating Materialized View For All Time Rating By Year
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW gojek_rating_view AS
 SELECT EXTRACT(year FROM score_date) AS year,
    round(avg(score), 2) AS average_rating,
    count(*) AS total_reviews
   FROM gojekreview
  GROUP BY (EXTRACT(year FROM score_date))
  ORDER BY (EXTRACT(year FROM score_date)) DESC;

-- Creating Materialized View For All Time Rating By Month
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW average_rating_over_time AS
 SELECT
    TO_CHAR(score_date, 'Month YYYY') AS month_year,
    ROUND(AVG(score), 2) AS average_rating
   FROM
    gojekreview
  GROUP BY
    TO_CHAR(score_date, 'Month YYYY')
  ORDER BY
    MIN(score_date) ASC;

-- Creating Materialized View For Sentiment Based On Positive or Negative Keywords
-----------------------------------------------------------------------
CREATE MATERIALIZED VIEW sentimen AS
 SELECT
        CASE
            WHEN topic = ANY (ARRAY['bagus'::text, 'puas'::text, 'suka'::text, 'baik'::text, 'mantap'::text, 'recommended'::text, 'love'::text]) THEN 'positif'::text
            ELSE 'negatif'::text
        END AS sentiment_category,
    sum(mentions) AS total_mentions
   FROM ( SELECT
                CASE
                    WHEN lower(gojekreview.content) ~~ '%bagus%'::text THEN 'bagus'::text
                    WHEN lower(gojekreview.content) ~~ '%puas%'::text THEN 'puas'::text
                    WHEN lower(gojekreview.content) ~~ '%suka%'::text THEN 'suka'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak suka%'::text THEN 'tidak suka'::text
                    WHEN lower(gojekreview.content) ~~ '%benci%'::text THEN 'benci'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak puas%'::text THEN 'tidak puas'::text
                    WHEN lower(gojekreview.content) ~~ '%blok%'::text THEN 'blok'::text
                    WHEN lower(gojekreview.content) ~~ '%ban%'::text THEN 'ban'::text
                    WHEN lower(gojekreview.content) ~~ '%baik%'::text THEN 'baik'::text
                    WHEN lower(gojekreview.content) ~~ '%mantap%'::text THEN 'mantap'::text
                    WHEN lower(gojekreview.content) ~~ '%recommended%'::text THEN 'recommended'::text
                    WHEN lower(gojekreview.content) ~~ '%love%'::text THEN 'love'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak baik%'::text THEN 'tidak baik'::text
                    WHEN lower(gojekreview.content) ~~ '%buruk%'::text THEN 'buruk'::text
                    WHEN lower(gojekreview.content) ~~ '%jelek%'::text THEN 'jelek'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak rekomen%'::text THEN 'tidak rekomen'::text
                    ELSE NULL::text
                END AS topic,
            count(*) AS mentions
           FROM gojekreview
          WHERE lower(gojekreview.content) ~~ '%bagus%'::text OR lower(gojekreview.content) ~~ '%puas%'::text OR lower(gojekreview.content) ~~ '%suka%'::text OR lower(gojekreview.content) ~~ '%tidak suka%'::text OR lower(gojekreview.content) ~~ '%benci%'::text OR lower(gojekreview.content) ~~ '%tidak puas%'::text OR lower(gojekreview.content) ~~ '%blok%'::text OR lower(gojekreview.content) ~~ '%ban%'::text OR lower(gojekreview.content) ~~ '%baik%'::text OR lower(gojekreview.content) ~~ '%mantap%'::text OR lower(gojekreview.content) ~~ '%recommended%'::text OR lower(gojekreview.content) ~~ '%love%'::text OR lower(gojekreview.content) ~~ '%tidak baik%'::text OR lower(gojekreview.content) ~~ '%buruk%'::text OR lower(gojekreview.content) ~~ '%jelek%'::text OR lower(gojekreview.content) ~~ '%tidak rekomen%'::text
          GROUP BY (
                CASE
                    WHEN lower(gojekreview.content) ~~ '%bagus%'::text THEN 'bagus'::text
                    WHEN lower(gojekreview.content) ~~ '%puas%'::text THEN 'puas'::text
                    WHEN lower(gojekreview.content) ~~ '%suka%'::text THEN 'suka'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak suka%'::text THEN 'tidak suka'::text
                    WHEN lower(gojekreview.content) ~~ '%benci%'::text THEN 'benci'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak puas%'::text THEN 'tidak puas'::text
                    WHEN lower(gojekreview.content) ~~ '%blok%'::text THEN 'blok'::text
                    WHEN lower(gojekreview.content) ~~ '%ban%'::text THEN 'ban'::text
                    WHEN lower(gojekreview.content) ~~ '%baik%'::text THEN 'baik'::text
                    WHEN lower(gojekreview.content) ~~ '%mantap%'::text THEN 'mantap'::text
                    WHEN lower(gojekreview.content) ~~ '%recommended%'::text THEN 'recommended'::text
                    WHEN lower(gojekreview.content) ~~ '%love%'::text THEN 'love'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak baik%'::text THEN 'tidak baik'::text
                    WHEN lower(gojekreview.content) ~~ '%buruk%'::text THEN 'buruk'::text
                    WHEN lower(gojekreview.content) ~~ '%jelek%'::text THEN 'jelek'::text
                    WHEN lower(gojekreview.content) ~~ '%tidak rekomen%'::text THEN 'tidak rekomen'::text
                    ELSE NULL::text
                END)) topics
  GROUP BY (
        CASE
            WHEN topic = ANY (ARRAY['bagus'::text, 'puas'::text, 'suka'::text, 'baik'::text, 'mantap'::text, 'recommended'::text, 'love'::text]) THEN 'positif'::text
            ELSE 'negatif'::text
        END)
  ORDER BY (sum(mentions)) DESC;
