-- Задача: Создать материализованное представление для хранения информации о маршрутах
-- Контекст: Обеспечить централизованное и актуальное хранилище маршрутов для планирования рейсов.

-- Таблица-приёмник для маршрутов
CREATE TABLE vj_volov_routes_storage
(
    flight_no String,
    departure_airport String,
    arrival_airport String,
    aircraft_code String,
    duration UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY flight_no;

-- Материализованное представление
CREATE MATERIALIZED VIEW vj_volov_routes
TO vj_volov_routes_storage
AS
SELECT
    flight_no,
    any(departure_airport) AS departure_airport,
    any(arrival_airport) AS arrival_airport,
    any(aircraft_code) AS aircraft_code,
    any(toUInt32(scheduled_arrival - scheduled_departure)) AS duration
FROM vj_volov_flights
GROUP BY flight_no;

-- Перезагрузка данных для активации MV
CREATE TABLE IF NOT EXISTS vj_volov_flights_backup
ENGINE = MergeTree ORDER BY flight_id
AS SELECT * FROM vj_volov_flights;

TRUNCATE TABLE vj_volov_flights;

INSERT INTO vj_volov_flights
SELECT * FROM vj_volov_flights_backup;

-- Проверка данных
SELECT * FROM vj_volov_routes_storage LIMIT 5;

--PG0001	UIK	SGC	CR2	8400
--PG0002	SGC	UIK	CR2	8400
--PG0003	IWA	AER	CR2	7800
--PG0004	AER	IWA	CR2	7800
--PG0005	DME	PKV	CN1	7500