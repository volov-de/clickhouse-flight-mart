-- Задача: Оптимизация витрины по производительности
-- Контекст: Ускорить аналитические запросы за счёт партиционирования и индексов.

CREATE TABLE vj_volov_fct_flights_weather_mart_opt (
    flight_id UInt32,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport String,
    arrival_airport String,
    status String,
    aircraft_code String,
    actual_departure DateTime,
    actual_arrival DateTime,
    fare_conditions String,
    amount String,
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    contact_data String,
    book_date String,
    total_amount String,
    departure_temperature Float32,
    departure_humidity UInt8,
    departure_wind_speed Float32,
    departure_condition String,
    arrival_temperature Float32,
    arrival_humidity UInt8,
    arrival_wind_speed Float32,
    arrival_condition String,
    INDEX airports_idx (departure_airport, arrival_airport) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(scheduled_departure)
PRIMARY KEY flight_id
ORDER BY flight_id
SETTINGS index_granularity = 8192;

-- Загрузка данных
INSERT INTO vj_volov_fct_flights_weather_mart_opt
SELECT * FROM vj_volov_fct_flights_weather_mart;

-- Проверка структуры и партиций
DESCRIBE TABLE vj_volov_fct_flights_weather_mart_opt;

SELECT count(*) AS total_records FROM vj_volov_fct_flights_weather_mart_opt;

SELECT
    partition,
    count(*) AS rows_count
FROM system.parts
WHERE table = 'vj_volov_fct_flights_weather_mart_opt' AND active = 1
GROUP BY partition
ORDER BY partition;