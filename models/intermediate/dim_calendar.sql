WITH date_spine_raw AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="current_date - interval '4 months'",
        end_date="current_date"
    ) }}
),

daily_spine AS (
    SELECT 
        date_day,
        'daily' as granularity,
        (date_day::timestamp AT TIME ZONE 'UTC')::timestamptz as period_start,
        ((date_day + interval '1 day - 1 second')::timestamp AT TIME ZONE 'UTC')::timestamptz as period_end,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Month') as month_name
    FROM date_spine_raw
),

monthly_spine AS (
    SELECT DISTINCT
        date_trunc('month', date_day) as date_day,
        'monthly' as granularity,
        (date_trunc('month', date_day)::timestamp AT TIME ZONE 'UTC')::timestamptz as period_start,
        ((date_trunc('month', date_day) + interval '1 month - 1 second')::timestamp AT TIME ZONE 'UTC')::timestamptz as period_end,
        extract(year from date_trunc('month', date_day)) as year,
        extract(month from date_trunc('month', date_day)) as month,
        null::integer as day,
        null::integer as day_of_week,
        null::text as day_name,
        to_char(date_trunc('month', date_day), 'Month') as month_name
    FROM date_spine_raw
)

SELECT * FROM daily_spine
UNION ALL
SELECT * FROM monthly_spine
ORDER BY date_day, granularity