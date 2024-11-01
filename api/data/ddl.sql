CREATE TABLE delayed_flights (
    departure_date DATE,
    departure_delay_count INT,
    arrival_delay_count INT
);

CREATE TABLE airports (
    airport_id SERIAL PRIMARY KEY,
    iata_code VARCHAR(16),
    icao_code VARCHAR(16),
    faa_code VARCHAR(16),
    name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE airlines (
    airline_id SERIAL PRIMARY KEY,
    iata_code VARCHAR(16),
    icao_code VARCHAR(16),
    name VARCHAR(100)
);
