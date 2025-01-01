-- Predaje skladieb a výnosy podľa žánru
SELECT
    g.genre_name AS Genre,
    t.track_name AS Track,
    SUM(f.Quantity) AS Total_Sales,
    SUM(f.Total) AS Total_Revenue
FROM etl_staging.fact_sales f
    JOIN etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId
    JOIN etl_staging.dim_track_genre tg ON t.trackId = tg.dim_track_trackId
    JOIN etl_staging.dim_genres g ON tg.dim_genre_genreId = g.genreId
GROUP BY
    g.genre_name,
    t.track_name
ORDER BY Total_Revenue DESC;


-- Štatistiky výkonu zamestnancov
SELECT
    ROW_NUMBER() OVER (
        ORDER BY e.employeeId
    ) AS Employee_ID,
    e.employee_age AS Age, 
    e.employee_nationality AS Nationality,
    COUNT(DISTINCT f.salesId) AS Total_Interactions, 
    SUM(f.Total) AS Total_Revenue
FROM etl_staging.dim_employees e
    LEFT JOIN etl_staging.fact_sales f ON e.employeeId = f.employeeId
GROUP BY
    e.employeeId,
    e.employee_age,
    e.employee_nationality
ORDER BY Employee_ID ASC;

-- Štatistiky angažovanosti zákazníkov
SELECT
    c.customerId AS Customer_ID,
    c.customer_nationality AS Nationality,
    COUNT(f.salesId) AS Total_Purchases,
    SUM(f.Total) AS Total_Spending 
FROM etl_staging.fact_sales f
    JOIN etl_staging.dim_customers c ON f.customerId = c.customerId
GROUP BY
    c.customerId,
    c.customer_nationality
ORDER BY Total_Spending DESC;

-- Štatistiky výkonu skladateľov
SELECT
    t.track_author AS Composer,
    COUNT(DISTINCT f.salesId) AS Total_Tracks_Sold,
    SUM(f.Total) AS Total_Revenue,
    ROUND(AVG(f.Total), 2) AS Avg_Revenue_Per_Track
FROM etl_staging.fact_sales f
    JOIN etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId
WHERE
    t.track_author IS NOT NULL
GROUP BY
    t.track_author
ORDER BY Total_Revenue DESC;

-- Štatistiky ročných výnosov podľa žánru
SELECT g.genre_name AS Genre,
    d.year AS Year,
    SUM(f.Total) AS Total_Revenue
FROM
    etl_staging.fact_sales f
    JOIN etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId
    JOIN etl_staging.dim_track_genre tg ON t.trackId = tg.dim_track_trackId
    JOIN etl_staging.dim_genres g ON tg.dim_genre_genreId = g.genreId
    JOIN etl_staging.dim_date d ON f.dateId = d.dateId
GROUP BY
    g.genre_name,
    d.year
ORDER BY d.year, Total_Revenue DESC;
