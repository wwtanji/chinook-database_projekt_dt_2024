CREATE DATABASE cat_chinook;
CREATE SCHEMA etl_staging;

CREATE OR REPLACE STAGE my_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


CREATE OR REPLACE TABLE employee_staging (
    EmployeeId INT,
    LastName VARCHAR(20),
    FirstName VARCHAR(20),
    Title VARCHAR(30),
    ReportsTo INT,
    BirthDate DATETIME,
    HireDate DATETIME,
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60)
);

CREATE OR REPLACE TABLE customer_staging (
    CustomerId INT,
    FirstName VARCHAR(40),
    LastName VARCHAR(20),
    Company VARCHAR(80),
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60),
    SupportRepId INT
);

CREATE OR REPLACE TABLE invoice_staging (
    InvoiceId INT,
    CustomerId INT,
    InvoiceDate DATETIME,
    BillingAddress VARCHAR(70),
    BillingCity VARCHAR(40),
    BillingState VARCHAR(40),
    BillingCountry VARCHAR(40),
    BillingPostalCode VARCHAR(10),
    Total DECIMAL(10,2)
);

CREATE OR REPLACE TABLE invoiceline_staging (
    InvoiceLineId INT,
    InvoiceId INT,
    TrackId INT,
    UnitPrice DECIMAL(10,2),
    Quantity INT
);

CREATE OR REPLACE TABLE track_staging (
    TrackId INT,
    Name VARCHAR(200),
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer VARCHAR(220),
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL(10,2)
);

CREATE OR REPLACE TABLE mediatype_staging (
    MediaTypeId INT,
    Name VARCHAR(120)
);

CREATE OR REPLACE TABLE genre_staging (
    GenreId INT,
    Name VARCHAR(120)
);

CREATE OR REPLACE TABLE playlist_staging (
    PlaylistId INT,
    Name VARCHAR(120)
);

CREATE OR REPLACE TABLE playlisttrack_staging (
    PlaylistId INT,
    TrackId INT
);

CREATE OR REPLACE TABLE album_staging (
    AlbumId INT,
    Title VARCHAR(160),
    ArtistId INT
);

CREATE OR REPLACE TABLE artist_staging (
    ArtistId INT,
    Name VARCHAR(120)
);

COPY INTO album_staging
FROM @my_stage/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO artist_staging
FROM @my_stage/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customer_staging
FROM @my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employee_staging
FROM @my_stage/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genre_staging
FROM @my_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoiceline_staging
FROM @my_stage/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoice_staging
FROM @my_stage/invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO mediatype_staging
FROM @my_stage/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlisttrack_staging
FROM @my_stage/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlist_staging
FROM @my_stage/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO track_staging
FROM @my_stage/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


SELECT * FROM album_staging;
SELECT * FROM artist_staging;
SELECT * FROM customer_staging;
SELECT * FROM employee_staging;
SELECT * FROM genre_staging;
SELECT * FROM invoiceline_staging;
SELECT * FROM invoice_staging;
SELECT * FROM mediatype_staging;
SELECT * FROM playlisttrack_staging;
SELECT * FROM track_staging;

--- Albums
CREATE OR REPLACE TABLE dim_albums AS
SELECT
    DISTINCT ALBUMID, 
    TITLE AS album_title,
    ARTISTID AS artistId
FROM album_staging;


--- Artists
CREATE OR REPLACE TABLE dim_artist AS
SELECT
    DISTINCT ARTISTID AS artistId,
    NAME as artist_name
FROM artist_staging;

--- Customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    CustomerId AS customerId,
    NULL AS customer_age,
    NULL AS customer_gender,
    Country AS customer_nationality
FROM customer_staging;

--- Employeess
CREATE OR REPLACE TABLE dim_employees AS
SELECT DISTINCT
    EmployeeId AS employeeId,
    DATEDIFF(YEAR, BirthDate, CURRENT_DATE) AS employee_age,
    Country AS employee_nationality,
    NULL AS employee_gender
FROM employee_staging;


--- Track + Genre
CREATE OR REPLACE TABLE dim_track_genre AS
SELECT DISTINCT
    t.TrackId AS dim_track_trackId,
    t.GenreId AS dim_genre_genreId
FROM track_staging t
JOIN genre_staging g
ON t.GenreId = g.GenreId;


--- Tracks
CREATE OR REPLACE TABLE dim_tracks AS
SELECT DISTINCT
    t.TrackId AS trackId,
    t.Name AS track_name,
    t.Composer AS track_author,
    t.AlbumId AS albumId,
    a.ALBUM_TITLE AS playlistName
FROM track_staging t
LEFT JOIN dim_albums a
ON t.AlbumId = a.ALBUMID;


--- Genres

CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    GENREID AS genreId,
    NAME AS genre_name,
FROM GENRE_STAGING;

--- Addresses
CREATE OR REPLACE TABLE dim_addresses AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY ADDRESS) AS adressId,
    SPLIT_PART(ADDRESS, ' ', 1) AS street,
    POSTALCODE AS postal_code,
    CITY AS city,
    STATE AS state
FROM employee_staging
WHERE ADDRESS IS NOT NULL;

--- Time
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY InvoiceDate) AS timeId,
    EXTRACT(HOUR FROM CAST(InvoiceDate AS TIMESTAMP)) AS hour, 
    EXTRACT(MINUTE FROM CAST(InvoiceDate AS TIMESTAMP)) AS minute, 
    EXTRACT(SECOND FROM CAST(InvoiceDate AS TIMESTAMP)) AS second, 
    CAST(InvoiceDate AS DATE) AS purchase_date 
FROM invoice_staging;

--- Date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(InvoiceDate AS DATE)) AS dateId,
    CAST(InvoiceDate AS DATE) AS date,
    EXTRACT(DAY FROM CAST(InvoiceDate AS DATE)) AS day,
    EXTRACT(MONTH FROM CAST(InvoiceDate AS DATE)) AS month,
    EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) AS year,
    CEIL(EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) / 100) AS century, 
    FLOOR(EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) / 10) AS decade,
    EXTRACT(QUARTER FROM CAST(InvoiceDate AS DATE)) AS quarter
FROM invoice_staging;

-- Sales
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceId) AS salesId, -- unique identifier for the sale
    il.Quantity AS Quantity, -- quantity of items
    il.UnitPrice AS UnitPrice, -- unit price of the item
    il.Quantity * il.UnitPrice AS Total, -- total amount
    d.dateId AS dateId, -- date identifier (from the `dim_date` table)
    dt.timeId AS timeId, -- time identifier (from the `dim_time` table)
    c.CustomerId AS customerId, -- customer identifier
    e.EmployeeId AS employeeId, -- employee identifier
    a.adressId AS adressId, -- address identifier
    t.TrackId AS dim_tracks_trackId -- track identifier
FROM
    invoice_staging i
JOIN
    invoiceline_staging il ON i.InvoiceId = il.InvoiceId -- join with invoice line details
JOIN
    customer_staging c ON i.CustomerId = c.CustomerId -- join with customers
LEFT JOIN
    employee_staging e ON c.SupportRepId = e.EmployeeId -- join with employees (if available)
JOIN
    track_staging t ON il.TrackId = t.TrackId -- join with tracks
LEFT JOIN
    dim_addresses a ON i.BillingAddress = a.street -- join with addresses (if available)
LEFT JOIN
    dim_date d ON CAST(i.InvoiceDate AS DATE) = d.date -- join with the date table
LEFT JOIN
    dim_time dt ON EXTRACT(HOUR FROM i.InvoiceDate) = dt.hour
                 AND EXTRACT(MINUTE FROM i.InvoiceDate) = dt.minute
                 AND EXTRACT(SECOND FROM i.InvoiceDate) = dt.second; -- join with the time table


DROP TABLE IF EXISTS album_staging;
DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS customer_staging;
DROP TABLE IF EXISTS employee_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS invoiceline_staging;
DROP TABLE IF EXISTS invoice_staging;
DROP TABLE IF EXISTS mediatype_staging;
DROP TABLE IF EXISTS playlisttrack_staging;
DROP TABLE IF EXISTS track_staging;


-- Analysis
CREATE SCHEMA analysis;
USE SCHEMA CAT_CHINOOK.ANALYSIS;

CREATE OR REPLACE VIEW analysis.genre_track_sales_stats AS
SELECT
    g.genre_name AS Genre, -- Назва жанру
    t.track_name AS Track, -- Назва треку
    SUM(f.Quantity) AS Total_Sales, -- Загальна кількість проданих одиниць
    SUM(f.Total) AS Total_Revenue -- Загальна виручка
FROM etl_staging.fact_sales f -- Використовується правильна схема
    JOIN etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId -- З'єднання з таблицею треків
    JOIN etl_staging.dim_track_genre tg ON t.trackId = tg.dim_track_trackId -- З'єднання з таблицею жанрів-треків
    JOIN etl_staging.dim_genres g ON tg.dim_genre_genreId = g.genreId -- З'єднання з таблицею жанрів
GROUP BY
    g.genre_name,
    t.track_name
ORDER BY Total_Revenue DESC;

CREATE OR REPLACE VIEW analysis.employee_performance_stats AS
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employeeId) AS Employee_ID, -- Послідовний ID для працівників
    e.employee_age AS Age, -- Вік працівника
    e.employee_nationality AS Nationality, -- Національність працівника
    COUNT(DISTINCT f.salesId) AS Total_Interactions, -- Загальна кількість взаємодій із клієнтами
    SUM(f.Total) AS Total_Revenue -- Сумарний дохід від клієнтів
FROM etl_staging.dim_employees e
    LEFT JOIN etl_staging.fact_sales f ON e.employeeId = f.employeeId -- З'єднання з таблицею продажів
GROUP BY
    e.employeeId,
    e.employee_age,
    e.employee_nationality
ORDER BY Employee_ID ASC;

SELECT * FROM analysis.employee_performance_stats;

CREATE OR REPLACE VIEW analysis.customer_engagement_stats AS
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

CREATE OR REPLACE VIEW analysis.composer_performance_stats AS
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

SELECT * FROM analysis.composer_performance_stats;


CREATE OR REPLACE VIEW analysis.genre_yearly_revenue_stats AS
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

SELECT * FROM analysis.genre_yearly_revenue_stats;