-- Задача: Создать таблицу аэропортов и агрегационную таблицу по количеству вылетов и задержек
-- Контекст: Предоставить аналитикам агрегированные данные по аэропортам.

-- Создание таблицы аэропортов
CREATE TABLE vj_volov_airports
(
    airport_code String,
    airport_name String,
    city String,
    longitude Float64,
    latitude Float64,
    timezone String
)
ENGINE = MergeTree
ORDER BY airport_code;

-- Загрузка данных об аэропортах
INSERT INTO vj_volov_airports
SELECT *
FROM postgresql(
    'postgres:5432',
    'demo',
    'airports',
    'vj_volov',
    'secret',
    'bookings'
);

-- Проверка загрузки
SELECT * FROM vj_volov_airports LIMIT 5;

-- Создание агрегационной таблицы
CREATE TABLE vj_volov_flights_aggregates
(
    departure_airport String,
    total_flights UInt64,
    delayed_flights UInt64
)
ENGINE = SummingMergeTree
ORDER BY (departure_airport)
SETTINGS index_granularity = 8192;

-- Заполнение агрегатов
INSERT INTO vj_volov_flights_aggregates
SELECT
    f.departure_airport,
    COUNT(*) AS total_flights,
    SUM(
        CASE
            WHEN f.actual_departure > f.scheduled_departure THEN 1
            ELSE 0
        END
    ) AS delayed_flights
FROM vj_volov_flights f
GROUP BY f.departure_airport;

-- Проверка результатов
SELECT * FROM vj_volov_flights_aggregates LIMIT 10;

--departure_airport, total_flights, delayed_flights
--AAQ	849	751
--ASF	961	855
--NFG	283	251
--NUX	3282	2897
--SKX	792	695