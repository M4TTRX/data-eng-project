--
-- PostgreSQL database dump
--
CREATE TABLE IF NOT EXISTS power_plants (
    id VARCHAR PRIMARY KEY, 
    plant_name VARCHAR NOT NULL,
    plant_type VARCHAR NOT NULL,
    fuel_type VARCHAR NOT NULL,
    creation_date DATE NOT NULL,
    plant_power DECIMAL NOT NULL,
    latitude DECIMAL,
    longitude DECIMAL
);