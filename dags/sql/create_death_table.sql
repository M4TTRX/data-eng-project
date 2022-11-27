--
-- PostgreSQL database dump
--
CREATE TABLE IF NOT EXISTS deaths (
    id VARCHAR PRIMARY KEY,
    date_of_birth DATE NOT NULL,
    date_of_death DATE NOT NULL,
    latitude DECIMAL,
    longitude DECIMAL
);