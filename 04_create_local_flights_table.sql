-- Задача: Создать локальную таблицу flights с движком MergeTree и партицированием по месяцу вылета
-- Контекст: Обеспечить быструю загрузку и эффективное хранение данных в ClickHouse.

CREATE TABLE vj_volov_flights
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
ORDER BY (flight_id)
SETTINGS index_granularity = 8192;

-- Загрузка данных из реплицированной таблицы
INSERT INTO vj_volov_flights
SELECT * FROM vj_volov_flights_remote;

-- Проверка: количество загруженных строк
SELECT count(*) AS row_count FROM vj_volov_flights;

-- Проверка: распределение по партициям
SELECT
    partition,
    sum(rows) AS rows_in_partition
FROM system.parts
WHERE database = 'student_data' AND table = 'vj_volov_flights'
GROUP BY partition
ORDER BY partition;

--partition	rows_in_partition
--201510	9780
--201511	16254
--201512	16831
--201601	16783
--201602	15760
--201603	16831
--201604	16289
--201605	16811
--201606	16274
--201607	16783
--201608	16853
--201609	16286
--201610	16803
--201611	6529
