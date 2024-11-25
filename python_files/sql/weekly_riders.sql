WITH weekly_ridership AS (
    SELECT 
        station_complex, 
        DATE_TRUNC('week', transit_timestamp) AS week_start,
        SUM(ridership) AS total_weekly_ridership,
        MIN(latitude) AS latitude,  -- Assuming latitude is the same for each station complex, use MIN() or MAX()
        MIN(longitude) AS longitude  -- Assuming longitude is the same for each station complex, use MIN() or MAX()
    FROM 
        mta_hourly_subway_socrata
    GROUP BY 
        station_complex, 
        DATE_TRUNC('week', transit_timestamp)
),
weekly_weather AS (
    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        AVG(temperature_mean) AS avg_weekly_temperature,
        SUM(precipitation_sum) AS total_weekly_precipitation
    FROM 
        daily_weather_asset
    GROUP BY 
        DATE_TRUNC('week', date)
)
SELECT 
    wr.station_complex, 
    wr.week_start, 
    wr.total_weekly_ridership,
    wr.latitude,
    wr.longitude,
    ww.avg_weekly_temperature,
    ww.total_weekly_precipitation
FROM 
    weekly_ridership wr
LEFT JOIN 
    weekly_weather ww
ON 
    wr.week_start = ww.week_start
WHERE 
    wr.week_start < '2024-09-17'
ORDER BY 
    wr.station_complex, 
    wr.week_start