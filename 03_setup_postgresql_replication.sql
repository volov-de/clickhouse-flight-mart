-- Задача: Настроить репликацию из PostgreSQL — создать таблицу-реплику flights_remote
-- Контекст: Организовать синхронизацию данных о рейсах из внешней системы (PostgreSQL).

DROP TABLE IF EXISTS vj_volov_flights_remote;

CREATE TABLE vj_volov_flights_remote
(
    `flight_id` UInt32,
    `flight_no` String,
    `scheduled_departure` DateTime,
    `scheduled_arrival` DateTime,
    `departure_airport` String,
    `arrival_airport` String,
    `status` String,
    `aircraft_code` String,
    `actual_departure` Nullable(DateTime),
    `actual_arrival` Nullable(DateTime)
)
ENGINE = PostgreSQL(
    'host:5432',
    'demo',
    'flights',
    'vj_volov',
    'pass',
    'bookings'
);