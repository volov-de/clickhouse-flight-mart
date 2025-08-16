-- Задача: Подсчитать количество уникальных пассажиров, чей рейс был задержан более чем на 3 часа
-- Контекст: Данные требуются для расчёта компенсаций и анализа качества обслуживания.

SELECT
    COUNT(DISTINCT t.passenger_id) AS delayed_passengers
FROM
    flights f
JOIN
    ticket_flights tf ON f.flight_id = tf.flight_id
JOIN
    tickets t ON tf.ticket_no = t.ticket_no
WHERE
    dateDiff('second', scheduled_departure, actual_departure) > 10800;