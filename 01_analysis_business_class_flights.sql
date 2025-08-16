-- Задача: Определить месяц 2016 года с наибольшим количеством перелётов бизнес-класса
-- Контекст: Подготовить отчёт для отдела качества обслуживания по популярности бизнес-класса.

SELECT
    toMonth(scheduled_departure) AS month,
    count(*) AS flight_count
FROM
    flights AS f
JOIN
    seats AS s ON s.aircraft_code = f.aircraft_code
WHERE
    s.fare_conditions = 'Business'
    AND scheduled_departure >= '2016-01-01'
    AND scheduled_departure < '2017-01-01'
GROUP BY
    month
ORDER BY
    flight_count DESC
LIMIT 1;

-- Ответ: Июль