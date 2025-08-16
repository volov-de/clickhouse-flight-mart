-- Задача: Оптимизация хранения — уменьшение размера таблицы
-- Контекст: Минимизировать объём данных для снижения затрат на хранение и ускорения чтения.

DROP TABLE IF EXISTS vj_volov_fct_flights_weather_mart_opt;

CREATE TABLE vj_volov_fct_flights_weather_mart_opt
(
    `ticket_no` FixedString(13),
    `book_ref` FixedString(6),
    `passenger_id` String,
    `passenger_name` String,
    `passenger_email` String,
    `passenger_phone` String,
    `flight_id` UInt32,
    `fare_conditions` LowCardinality(String),
    `amount` Float32,
    `flight_no` FixedString(6),
    `scheduled_departure` DateTime CODEC(Delta(4), ZSTD(1)),
    `scheduled_arrival` DateTime CODEC(Delta(4), ZSTD(1)),
    `departure_airport` LowCardinality(FixedString(3)),
    `arrival_airport` LowCardinality(FixedString(3)),
    `status` Enum8('Scheduled' = 1, 'On Time' = 2, 'Delayed' = 3, 'Departed' = 4, 'Arrived' = 5, 'Cancelled' = 6),
    `aircraft_code` LowCardinality(FixedString(3)),
    `actual_departure` Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    `actual_arrival` Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    `book_date` DateTime CODEC(Delta(4), ZSTD(1)),
    `total_amount` Float32,
    `departure_temperature` Float32 CODEC(Delta(4), ZSTD(1)),
    `departure_humidity` UInt8,
    `departure_wind_speed` Float32,
    `departure_condition` Enum8('Clear' = 1, 'Rain' = 2, 'Cloudy' = 3, 'Snow' = 4, 'Thunderstorm' = 5),
    `arrival_temperature` Float32 CODEC(Delta(4), ZSTD(1)),
    `arrival_humidity` UInt8,
    `arrival_wind_speed` Float32,
    `arrival_condition` Enum8('Clear' = 1, 'Rain' = 2, 'Cloudy' = 3, 'Snow' = 4, 'Thunderstorm' = 5),
    INDEX airports_idx (departure_airport, arrival_airport) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
PRIMARY KEY flight_id
ORDER BY (flight_id, scheduled_departure, departure_airport, arrival_airport)
SETTINGS index_granularity = 8192;

-- Загрузка с преобразованием
INSERT INTO vj_volov_fct_flights_weather_mart_opt (
    ticket_no, book_ref, passenger_id, passenger_name,
    passenger_email, passenger_phone, flight_id, fare_conditions,
    amount, flight_no, scheduled_departure, scheduled_arrival,
    departure_airport, arrival_airport, status, aircraft_code,
    actual_departure, actual_arrival, book_date, total_amount,
    departure_temperature, departure_humidity, departure_wind_speed, departure_condition,
    arrival_temperature, arrival_humidity, arrival_wind_speed, arrival_condition
)
SELECT
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    JSONExtractString(contact_data, 'email') AS passenger_email,
    JSONExtractString(contact_data, 'phone') AS passenger_phone,
    flight_id,
    fare_conditions,
    toFloat32(amount),
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    parseDateTime64BestEffort(book_date) AS book_date,
    toFloat32(total_amount),
    departure_temperature,
    departure_humidity,
    departure_wind_speed,
    departure_condition,
    arrival_temperature,
    arrival_humidity,
    arrival_wind_speed,
    arrival_condition
FROM vj_volov_fct_flights_weather_mart;

-- Финальные проверки
DESCRIBE TABLE vj_volov_fct_flights_weather_mart_opt;

SELECT count(*) AS total_records FROM vj_volov_fct_flights_weather_mart_opt;

-- Проверка размера
SELECT
    table,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE table = 'vj_volov_fct_flights_weather_mart_opt' AND active = 1
GROUP BY table;

-- Пример данных
SELECT
    ticket_no, book_ref, passenger_name, passenger_email,
    passenger_phone, flight_id, fare_conditions, amount,
    scheduled_departure, departure_airport, arrival_airport, status
FROM vj_volov_fct_flights_weather_mart_opt
LIMIT 5;