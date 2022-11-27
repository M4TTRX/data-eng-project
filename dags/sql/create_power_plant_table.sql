--
-- PostgreSQL database dump
--
CREATE TABLE IF NOT EXISTS power_plants (
    plant_name VARCHAR NOT NULL,
    plant_type VARCHAR NOT NULL,
    fuel_type VARCHAR NOT NULL,
    creation_date DATE NOT NULL,
    date_of_death DATE NOT NULL,
    plant_power DECIMAL NOT NULL,
    latitude DECIMAL,
    longitude DECIMAL
);