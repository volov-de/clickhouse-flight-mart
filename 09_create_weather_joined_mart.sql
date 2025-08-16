-- Задача: Создать витрину с погодными данными
-- Контекст: Проанализировать влияние погоды на задержки рейсов.

CREATE TABLE vj_volov_fct_flights_weather_mart (
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
    arrival_condition String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no, flight_id);

-- Загрузка с ASOF JOIN по погоде
INSERT INTO vj_volov_fct_flights_weather_mart
SELECT
    fm.flight_id,
    fm.flight_no,
    fm.scheduled_departure,
    fm.scheduled_arrival,
    fm.departure_airport,
    fm.arrival_airport,
    fm.status,
    fm.aircraft_code,
    coalesce(fm.actual_departure, toDateTime('1970-01-01 00:00:00')) AS actual_departure,
    coalesce(fm.actual_arrival, toDateTime('1970-01-01 00:00:00')) AS actual_arrival,
    fm.fare_conditions,
    fm.amount,
    fm.ticket_no,
    fm.book_ref,
    fm.passenger_id,
    fm.passenger_name,
    fm.contact_data,
    toString(fm.book_date) AS book_date,
    fm.total_amount,
    w_dep.temperature AS departure_temperature,
    w_dep.humidity AS departure_humidity,
    w_dep.wind_speed AS departure_wind_speed,
    w_dep.condition AS departure_condition,
    w_arr.temperature AS arrival_temperature,
    w_arr.humidity AS arrival_humidity,
    w_arr.wind_speed AS arrival_wind_speed,
    w_arr.condition AS arrival_condition
FROM vj_volov_fct_flights_mart fm
LEFT ASOF JOIN weather_data_hourly w_dep
    ON (fm.departure_airport = w_dep.airport AND fm.actual_departure >= w_dep.timestamp)
LEFT ASOF JOIN weather_data_hourly w_arr
    ON (fm.arrival_airport = w_arr.airport AND fm.actual_arrival >= w_arr.timestamp)
WHERE
    fm.status = 'Arrived'
    AND fm.actual_departure IS NOT NULL
    AND fm.actual_arrival IS NOT NULL
    AND fm.actual_departure > toDateTime('1970-01-01 00:00:00')
    AND fm.actual_arrival > toDateTime('1970-01-01 00:00:00');

-- Проверка
SELECT count(*) AS total_records FROM vj_volov_fct_flights_weather_mart;