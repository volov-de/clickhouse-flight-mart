-- Задача: Создать финальную витрину fct_flights_mart
-- Контекст: Объединить все данные в единую аналитическую таблицу для отчётов.

CREATE TABLE vj_volov_fct_flights_mart (
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    flight_id UInt32,
    fare_conditions String,
    amount String,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport String,
    arrival_airport String,
    status String,
    aircraft_code String,
    actual_departure DateTime,
    actual_arrival DateTime,
    book_date String,
    total_amount String,
    contact_data String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no, flight_id);

-- Заполнение витрины
INSERT INTO vj_volov_fct_flights_mart (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    flight_id,
    fare_conditions,
    amount,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    book_date,
    total_amount,
    contact_data
)
SELECT
    t.ticket_no,
    t.book_ref,
    t.passenger_id,
    t.passenger_name,
    toUInt32OrNull(tf.flight_id) AS flight_id,
    tf.fare_conditions,
    tf.amount,
    f.flight_no,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.departure_airport,
    f.arrival_airport,
    f.status,
    f.aircraft_code,
    coalesce(f.actual_departure, toDateTime('1970-01-01 00:00:00')) AS actual_departure,
    coalesce(f.actual_arrival, toDateTime('1970-01-01 00:00:00')) AS actual_arrival,
    toString(b.book_date) AS book_date,
    b.total_amount,
    t.contact_data
FROM vj_volov_bookings_raw b
JOIN vj_volov_tickets_raw t ON b.book_ref = t.book_ref
JOIN vj_volov_ticket_flights_raw tf ON t.ticket_no = tf.ticket_no
LEFT JOIN vj_volov_flights f ON toUInt32OrNull(tf.flight_id) = f.flight_id;

-- Проверка количества записей
SELECT count(*) AS total_records FROM vj_volov_fct_flights_mart;