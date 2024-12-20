CREATE DATABASE cat_chinook;

CREATE SCHEMA etl_staging;

CREATE OR REPLACE STAGE my_stage 
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Creating staging tables
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

-- Loading data into staging tables
COPY INTO album_staging
FROM @my_stage/album.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO artist_staging
FROM @my_stage/artist.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO customer_staging
FROM @my_stage/customer.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO employee_staging
FROM @my_stage/employee.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO genre_staging
FROM @my_stage/genre.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO invoiceline_staging
FROM @my_stage/invoiceline.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO invoice_staging
FROM @my_stage/invoice.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO mediatype_staging
FROM @my_stage/mediatype.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO playlisttrack_staging
FROM @my_stage/playlisttrack.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO playlist_staging
FROM @my_stage/playlist.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

COPY INTO track_staging
FROM @my_stage/track.csv
FILE_FORMAT = (
    TYPE = 'CSV', 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
    SKIP_HEADER = 1
);

-- Creating dimensions
-- Albums
CREATE OR REPLACE TABLE dim_albums AS
SELECT
    DISTINCT AlbumId, 
    Title AS album_title,
    ArtistId AS artist_id
FROM album_staging;

-- Artists
CREATE OR REPLACE TABLE dim_artist AS
SELECT
    DISTINCT ArtistId AS artist_id,
    Name AS artist_name
FROM artist_staging;

-- Customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    CustomerId AS customer_id,
    NULL AS customer_age,
    NULL AS customer_gender,
    Country AS customer_nationality
FROM customer_staging;

-- Employees
CREATE OR REPLACE TABLE dim_employees AS
SELECT DISTINCT
    EmployeeId AS employee_id,
    DATEDIFF(YEAR, BirthDate, CURRENT_DATE) AS employee_age,
    Country AS employee_nationality,
    NULL AS employee_gender
FROM employee_staging;

-- Track and Genre Mapping
CREATE OR REPLACE TABLE dim_track_genre AS
SELECT DISTINCT
    t.TrackId AS track_id,
    t.GenreId AS genre_id
FROM track_staging t
JOIN genre_staging g ON t.GenreId = g.GenreId;

-- Tracks
CREATE OR REPLACE TABLE dim_tracks AS
SELECT DISTINCT
    t.TrackId AS track_id,
    t.Name AS track_name,
    t.Composer AS track_author,
    t.AlbumId AS album_id,
    a.album_title
FROM track_staging t
LEFT JOIN dim_albums a ON t.AlbumId = a.AlbumId;

-- Genres
CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    GenreId AS genre_id,
    Name AS genre_name
FROM genre_staging;

-- Addresses
CREATE OR REPLACE TABLE dim_addresses AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY Address) AS address_id,
    SPLIT_PART(Address, ' ', 1) AS street,
    PostalCode AS postal_code,
    City AS city,
    State AS state
FROM employee_staging
WHERE Address IS NOT NULL;

-- Time Dimension
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY InvoiceDate) AS time_id,
    EXTRACT(HOUR FROM CAST(InvoiceDate AS TIMESTAMP)) AS hour, 
    EXTRACT(MINUTE FROM CAST(InvoiceDate AS TIMESTAMP)) AS minute, 
    EXTRACT(SECOND FROM CAST(InvoiceDate AS TIMESTAMP)) AS second, 
    CAST(InvoiceDate AS DATE) AS purchase_date 
FROM invoice_staging;

-- Date Dimension
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(InvoiceDate AS DATE)) AS date_id,
    CAST(InvoiceDate AS DATE) AS date,
    EXTRACT(DAY FROM CAST(InvoiceDate AS DATE)) AS day,
    EXTRACT(MONTH FROM CAST(InvoiceDate AS DATE)) AS month,
    EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) AS year,
    CEIL(EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) / 100) AS century, 
    FLOOR(EXTRACT(YEAR FROM CAST(InvoiceDate AS DATE)) / 10) AS decade,
    EXTRACT(QUARTER FROM CAST(InvoiceDate AS DATE)) AS quarter
FROM invoice_staging;

-- Fact Sales
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceId) AS sales_id,
    il.Quantity AS quantity,
    il.UnitPrice AS unit_price,
    il.Quantity * il.UnitPrice AS total,
    d.date_id AS date_id,
    dt.time_id AS time_id,
    c.CustomerId AS customer_id,
    e.EmployeeId AS employee_id,
    a.address_id AS address_id,
    t.TrackId AS track_id
FROM invoice_staging i
JOIN invoiceline_staging il ON i.InvoiceId = il.InvoiceId
JOIN customer_staging c ON i.CustomerId = c.CustomerId
LEFT JOIN employee_staging e ON c.SupportRepId = e.EmployeeId
JOIN track_staging t ON il.TrackId = t.TrackId
LEFT JOIN dim_addresses a ON i.BillingAddress = a.street
LEFT JOIN dim_date d ON CAST(i.InvoiceDate AS DATE) = d.date
LEFT JOIN dim_time dt ON EXTRACT(HOUR FROM i.InvoiceDate) = dt.hour
    AND EXTRACT(MINUTE FROM i.InvoiceDate) = dt.minute
    AND EXTRACT(SECOND FROM i.InvoiceDate) = dt.second;