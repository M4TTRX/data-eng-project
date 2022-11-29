--
-- PostgreSQL database dump
--
CREATE TABLE IF NOT EXISTS deaths (
    id VARCHAR PRIMARY KEY,
    date_of_birth DATE,
    date_of_death DATE,
    latitude DECIMAL,
    longitude DECIMAL
);