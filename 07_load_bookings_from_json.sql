-- Задача: Загрузить данные о бронированиях из JSON
-- Контекст: Организовать ETL-процесс для новых бронирований, поступающих в JSON-формате.

-- Таблица бронирований
CREATE TABLE vj_volov_bookings_raw (
    book_ref String,
    book_date DateTime64(0, 'UTC'),
    total_amount String
) ENGINE = MergeTree()
ORDER BY book_ref;

-- Таблица билетов
CREATE TABLE vj_volov_tickets_raw (
    ticket_no String,
    book_ref String,
    passenger_id String,
    passenger_name String,
    contact_data String
) ENGINE = MergeTree()
ORDER BY (book_ref, ticket_no);

-- Таблица связей билет-рейс
CREATE TABLE vj_volov_ticket_flights_raw (
    ticket_no String,
    flight_id String,
    fare_conditions String,
    amount String
) ENGINE = MergeTree()
ORDER BY (ticket_no, flight_id);

-- Загрузка бронирований
INSERT INTO vj_volov_bookings_raw (book_ref, book_date, total_amount)
SELECT DISTINCT
    book_ref,
    parseDateTime64BestEffort(book_date) AS book_date,
    toString(total_amount)
FROM bookings_json;

-- Загрузка билетов
INSERT INTO vj_volov_tickets_raw (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
SELECT DISTINCT
    JSONExtractString(ticket_data, 'ticket_no') AS ticket_no,
    book_ref,
    JSONExtractString(ticket_data, 'passenger_id') AS passenger_id,
    JSONExtractString(ticket_data, 'passenger_name') AS passenger_name,
    JSONExtractString(ticket_data, 'contact_data') AS contact_data
FROM (
    SELECT
        book_ref,
        ticket_data
    FROM bookings_json
    ARRAY JOIN JSONExtractArrayRaw(json_data) AS ticket_data
);

-- Загрузка связей билет-рейс
INSERT INTO vj_volov_ticket_flights_raw (ticket_no, flight_id, fare_conditions, amount)
SELECT DISTINCT
    ticket_no,
    JSONExtractString(flight_data, 'flight_id') AS flight_id,
    JSONExtractString(flight_data, 'fare_conditions') AS fare_conditions,
    JSONExtractString(flight_data, 'amount') AS amount
FROM (
    SELECT
        JSONExtractString(ticket_data, 'ticket_no') AS ticket_no,
        JSONExtractString(ticket_data, 'flights') AS flights_json
    FROM bookings_json
    ARRAY JOIN JSONExtractArrayRaw(json_data) AS ticket_data
) AS tickets_with_flights
ARRAY JOIN JSONExtractArrayRaw(flights_json) AS flight_data;

-- Проверка загрузки
SELECT * FROM vj_volov_bookings_raw LIMIT 10;
SELECT * FROM vj_volov_tickets_raw LIMIT 10;
SELECT * FROM vj_volov_ticket_flights_raw LIMIT 10;