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

CREATE TABLE flights (
    id SERIAL PRIMARY KEY,
    carrier_iata VARCHAR(10),
    flight_number INT,
    departure_airport_iata VARCHAR(10),
    departure_date_local DATE,
    departure_time_local TIME,
    arrival_airport_iata VARCHAR(10),
    arrival_date_local DATE,
    arrival_time_local TIME
);
