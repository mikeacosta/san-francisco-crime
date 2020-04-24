USE sfcrime;

CREATE TABLE police_districts(
    district_id INT, 
    company STRING, 
    district STRING
) stored as orc;

CREATE TABLE neighborhoods(
    neighborhood_id INT, 
    neighborhoods STRING
) stored as orc;

CREATE TABLE categories(
    category_id BIGINT, 
    category STRING
) stored as orc;

CREATE TABLE date_time(
    ts TIMESTAMP,
    hour INT,
    day INT,
    month INT,
    year INT,
    week INT
) stored as orc;

CREATE TABLE incidents(
    incident_id BIGINT,
    district_id INT,
    neighborhood_id INT,
    category_id BIGINT,
    incident_ts TIMESTAMP,
    report_ts TIMESTAMP,
    incident_number BIGINT,
    incident_description STRING,
    resolution STRING,
    intersection STRING,
    latitude FLOAT,
    longitude FLOAT,
    month INT,
    year INT
) stored as orc;