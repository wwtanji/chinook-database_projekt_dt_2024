# Implementácia ETL procesu pre analýzu dát z databázy Chinook

Tento repozitár obsahuje implementáciu ETL procesu pre analýzu dát z databázy Chinook. Proces zahŕňa kroky na extrahovanie, transformovanie a načítanie dát do dimenzionálneho modelu v Snowflake. Tento model umožňuje vizualizáciu a analýzu údajov o hudobných albumoch, skladbách, zákazníkoch a predajoch.

---
## 1. Úvod a popis zdrojových dát

Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa hudobných albumov, skladieb, playlistov a zákazníkov. Táto analýza umožňuje identifikovať trendy v hudobných preferenciách, najpopulárnejšie skladby a správanie používateľov pri vytváraní playlistov a nákupe hudby.


Zdrojové dáta pochádzajú z oficiálnej databázy Chinook, ktorá je dostupná na [GitHub odkaze](https://github.com/lerocha/chinook-database?tab=readme-ov-file) v sekcii **Download -> Latest Release**. Dataset obsahuje nasledujúce hlavné tabuľky:

- `album.csv`: Informácie o hudobných albumoch.
- `artist.csv`: Detaily o interpretoch.
- `customer.csv`: Informácie o zákazníkoch, vrátane ich kontaktných údajov.
- `employee.csv`: Dáta o zamestnancoch, ktorí spravujú objednávky.
- `genre.csv`: Žánre hudby.
- `invoice.csv`: Faktúry a informácie o predajoch.
- `invoiceline.csv`: Detaily o položkách na faktúrach.
- `mediatype.csv`: Typy médií, ako napríklad MP3 alebo AAC.
- `playlist.csv`: Zoznamy skladieb vytvorené používateľmi.
- `playlisttrack.csv`: Väzba medzi playlistami a skladbami.
- `track.csv`: Informácie o skladbách, vrátane názvov, žánrov a trvania. 


Účelom  ETL procesu bolo spracovať, upraviť a optimalizovať tieto dáta tak, aby boli vhodné na hlbšiu viacdimenzionálnu analýzu a vizualizáciu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Pôvodné dáta sú organizované v relačnom modeli, ktorý je zobrazený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/chinook_erd.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1: Entitno-relačná schéma Chinook</em>
</p>


---
## **2. Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_sales`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_genres`**: Obsahuje informácie o hudobných žánroch.
- **`dim_tracks`**: Poskytuje detaily o skladbách, vrátane názvov, autorov a prepojenia na albumy.
- **`dim_albums`**: Uchováva informácie o hudobných albumoch a ich prepojení s interpretmi.
- **`dim_artist`**: Obsahuje detaily o interpretoch, ako meno, vek, pohlavie a národnosť.
- **`dim_customers`**: Dáta o zákazníkoch, ako vek, pohlavie a národnosť.
- **`dim_employees`**: Údaje o zamestnancoch, ktorí spravujú predaje, vrátane veku a národnosti.
- **`dim_adresses`**: Adresné údaje, vrátane ulice, mesta, štátu a poštového kód.
- **`dim_time`**: Poskytuje podrobné časové údaje (hodina, minúta, sekunda) o transakciách
- **`dim_date`**: obsahuje hierarchické dátové údaje (deň, mesiac, rok, kvartál, dekáda).


Štruktúra hviezdicového modelu je zobrazené na diagrame nižšie. Diagram znázorňuje väzby medzi faktovou tabuľkou a dimenziami, čo uľahčuje porozumenie a realizáciu modelu.

<p align="center">
  <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2: Schéma hviezdy pre Chinook</em>
</p>
 
---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```

#### Načítanie údajov zo súborov do staging tabuliek

Ďalším krokom bolo nahratie obsahu jednotlivých .csv súborov do príslušných staging tabuliek. Pre každú tabuľku sa použili podobné príkazy.

1. Vytvorenie tabuľky

```sql
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
```

2. Importovanie dát

```sql
COPY INTO employee_staging
FROM @my_stage/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

3. Overovanie

```sql
SELECT * FROM employee_staging;
```
---

### 3.2 Transformácia

V tejto fáze sa vykonávalo čistenie, transformácia a obohacovanie údajov zo staging tabuliek. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku na jednoduchú a efektívnu analýzu.

---

## Vytvorenie dimenzionálnych tabuliek a faktovej tabuľky

- Dimenzia `dim_albums` bola vytvorená na základe tabuľky `album_staging`. Obsahuje unikátne informácie o albumoch, ich názvoch a pridružených umelcoch.

```sql
CREATE OR REPLACE TABLE dim_albums AS
SELECT
    DISTINCT ALBUMID, 
    TITLE AS album_title,
    ARTISTID AS artistId
FROM album_staging;
```

> **Typ dimenzie: _SCD1 (Slowly Changing Dimensions - Overwrite Old Value)_**<br>
> Informácie o albumoch môžu byť aktualizované bez uloženia historických zmien.
---

- `dim_artist` - Táto tabuľka obsahuje jedinečné informácie o umelcoch, vrátane ich identifikátorov a mien.

```sql
CREATE OR REPLACE TABLE dim_artist AS
SELECT
    DISTINCT ARTISTID AS artistId,
    NAME AS artist_name
FROM artist_staging;
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Pôvodná hodnota zostáva nemenná)_**<br>
> Mená umelcov sa považujú za statické.
---

- `dim_customers` - Táto tabuľka poskytuje podrobnosti o zákazníkoch, vrátane národnosti. Polia veku a pohlavia sú ponechané ako NULL z dôvodu chýbajúcich údajov.



```sql
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
    CustomerId AS customerId,
    NULL AS customer_age,
    NULL AS customer_gender,
    Country AS customer_nationality
FROM customer_staging;
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Pôvodná hodnota zostáva nemenná)_**<br>
> Demografické údaje zákazníkov sa v tomto súbore predpokladajú ako nemenné.

---
- `dim_employees` - Obsahuje podrobnosti o zamestnancoch, vrátane ich veku a národnosti. Pohlavie je ponechané ako NULL z dôvodu chýbajúcich údajov.

```sql
CREATE OR REPLACE TABLE dim_employees AS
SELECT DISTINCT
    EmployeeId AS employeeId,
    DATEDIFF(YEAR, BirthDate, CURRENT_DATE) AS employee_age,
    Country AS employee_nationality,
    NULL AS employee_gender
FROM employee_staging;
```

> **Typ dimenzie: _SCD1 (Slowly Changing Dimensions - Prepísanie starej hodnoty)_**<br>
> Údaje o zamestnancoch môžu byť aktualizované podľa potreby.
---

- `dim_tracks` - Obsahuje detaily o skladbách, vrátane názvov, autorov, pridružených albumov a playlistov.

Tabuľka bola vytvorená pomocou `LEFT JOIN` medzi tabuľkou `dim_albums a` a počiatočnou tabuľkou `track_staging t`.

```sql
CREATE OR REPLACE TABLE dim_tracks AS
SELECT DISTINCT
    t.trackId AS trackId,        
    t.track_name AS track_name,  
    t.track_author AS track_author,
    f.dim_albums_albumId AS albumId,
    a.album_name AS playlistName    
FROM dim_tracks t
LEFT JOIN fact_sales f
    ON t.trackId = f.dim_tracks_trackId
LEFT JOIN dim_albums a
    ON f.dim_albums_albumId = a.albumId; 
```
> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Zachovanie pôvodnej hodnoty)_**<br>
> Informácie o skladbách sú v tomto datasete statické.
---

- `dim_genres` - Obsahuje informácie o žánroch s unikátnymi identifikátormi a názvami.

```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    GENREID AS genreId,
    NAME AS genre_name
FROM GENRE_STAGING;
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Zachovanie pôvodnej hodnoty)_**<br>
> Predpokladá sa, že žánre sa v priebehu času nemenia.
---

- `dim_addresses` - Táto tabuľka poskytuje podrobnosti o adresách extrahované z tabuľky `employee_staging`.


```sql
CREATE OR REPLACE TABLE dim_addresses AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY ADDRESS) AS adressId,
    SPLIT_PART(ADDRESS, ' ', 1) AS street,
    POSTALCODE AS postal_code,
    CITY AS city,
    STATE AS state
FROM employee_staging
WHERE ADDRESS IS NOT NULL;
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Zachovanie pôvodnej hodnoty)_**<br>
> Informácie o adresách sú v tomto datasete statické a nemenia sa.

---
- `dim_time` - Extrahuje časové detaily, ako sú hodiny, minúty a sekundy, z dátumu faktúry.

```sql
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY InvoiceDate) AS timeId,
    EXTRACT(HOUR FROM CAST(InvoiceDate AS TIMESTAMP)) AS hour, 
    EXTRACT(MINUTE FROM CAST(InvoiceDate AS TIMESTAMP)) AS minute, 
    EXTRACT(SECOND FROM CAST(InvoiceDate AS TIMESTAMP)) AS second, 
    CAST(InvoiceDate AS DATE) AS purchase_date 
FROM invoice_staging;
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Zachovanie pôvodnej hodnoty)_**<br>
> Informácie o čase sú nemenné.
---
- `dim_date` - Obsahuje podrobné informácie o dátumoch na časovú analýzu.

```sql
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
```

> **Typ dimenzie: _SCD0 (Slowly Changing Dimensions - Zachovanie pôvodnej hodnoty)_**<br>
> Dátumy sa časom nemenia.
---
- `fact_sales` - Konsoliduje transakčné údaje s metrikami, ako sú Quantity, UnitPrice a Total. Obsahuje aj cudzie kľúče spájajúce príslušné dimenzionálne tabuľky.

```sql
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceId) AS salesId, 
    il.Quantity AS Quantity, 
    il.UnitPrice AS UnitPrice, 
    il.Quantity * il.UnitPrice AS Total,
    d.dateId AS dateId, 
    dt.timeId AS timeId, 
    c.CustomerId AS customerId,
    e.EmployeeId AS employeeId, 
    a.adressId AS adressId,
    t.TrackId AS dim_tracks_trackId 
FROM
    invoice_staging i
JOIN
    invoiceline_staging il ON i.InvoiceId = il.InvoiceId
JOIN
    customer_staging c ON i.CustomerId = c.CustomerId 
LEFT JOIN
    employee_staging e ON c.SupportRepId = e.EmployeeId 
JOIN
    track_staging t ON il.TrackId = t.TrackId
LEFT JOIN
    dim_addresses a ON i.BillingAddress = a.street 
LEFT JOIN
    dim_date d ON CAST(i.InvoiceDate AS DATE) = d.date 
LEFT JOIN
    dim_time dt ON EXTRACT(HOUR FROM i.InvoiceDate) = dt.hour
                 AND EXTRACT(MINUTE FROM i.InvoiceDate) = dt.minute
                 AND EXTRACT(SECOND FROM i.InvoiceDate) = dt.second;
```

> **Typ faktovej tabuľky**: _Additive Fact Table_<br>
> Predajné údaje je možné agregovať v rôznych dimenziách, ako sú zákazníci, zamestnanci, produkty, alebo čas.

---

### 3.3 Load

Po vytvorení dimenzií a faktovej tabuľky boli dáta presunuté do týchto tabuliek. Následne boli staging tabulky odstránené, aby sa optimalizovalo využitie úložiska.
```sql
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
```

### Výsledok ETL procesu

Výsledkom ETL procesu bolo rýchle a efektívne spracovanie `.csv` súborov pre vytvorenie definovaného multidimenzionálneho modelu typu star. Na ďalšiu analýzu boli vytvorené `View` v schéme `analysis`:

- `genre_track_sales_stats` - poskytuje prehľad o predaji jednotlivých hudobných žánrov a ich skladieb. Obsahuje údaje o počte predaných skladieb a celkových príjmoch, ktoré priniesli:

```sql
CREATE OR REPLACE VIEW analysis.genre_track_sales_stats AS
SELECT
    g.genre_name AS Genre, 
    t.track_name AS Track, 
    SUM(f.Quantity) AS Total_Sales, 
    SUM(f.Total) AS Total_Revenue 
FROM
    etl_staging.fact_sales f
JOIN
    etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId 
JOIN
    etl_staging.dim_track_genre tg ON t.trackId = tg.dim_track_trackId 
JOIN
    etl_staging.dim_genres g ON tg.dim_genre_genreId = g.genreId 
GROUP BY
    g.genre_name, t.track_name
ORDER BY
    Total_Revenue DESC;
```
- `employee_performance_stats` - poskytuje prehľad o výkone jednotlivých zamestnancov, vrátane ich veku, národnosti a celkových interakcií s klientmi. Obsahuje údaje o počte interakcií a generovaných príjmoch:

```sql
CREATE OR REPLACE VIEW analysis.employee_performance_stats AS
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employeeId) AS Employee_ID,
    e.employee_age AS Age,
    e.employee_nationality AS Nationality, 
    COUNT(DISTINCT f.salesId) AS Total_Interactions,
    SUM(f.Total) AS Total_Revenue
FROM
    etl_staging.dim_employees e
LEFT JOIN
    etl_staging.fact_sales f ON e.employeeId = f.employeeId 
GROUP BY
    e.employeeId, e.employee_age, e.employee_nationality 
ORDER BY
    Employee_ID ASC;
```

- `customer_engagement_stats` - poskytuje prehľad o angažovanosti zákazníkov, zobrazujúc informácie o ich nákupoch. Obsahuje údaje o počte nákupov a celkových výdavkoch jednotlivých zákazníkov:

```sql
CREATE OR REPLACE VIEW analysis.customer_engagement_stats AS
SELECT
    c.customerId AS Customer_ID,
    c.customer_nationality AS Nationality, 
    COUNT(f.salesId) AS Total_Purchases, 
    SUM(f.Total) AS Total_Spending 
FROM
    etl_staging.fact_sales f
JOIN
    etl_staging.dim_customers c ON f.customerId = c.customerId 
GROUP BY
    c.customerId, c.customer_nationality
ORDER BY
    Total_Spending DESC;
```

- `composer_performance_stats` - poskytuje prehľad o výkonnosti jednotlivých skladateľov (autorov skladieb). Zobrazuje počet predaných skladieb, celkové príjmy, ktoré priniesli, a priemerný príjem na jednu skladbu:
```sql
CREATE OR REPLACE VIEW analysis.composer_performance_stats AS
SELECT
    t.track_author AS Composer,
    COUNT(DISTINCT f.salesId) AS Total_Tracks_Sold, 
    SUM(f.Total) AS Total_Revenue, 
    ROUND(AVG(f.Total), 2) AS Avg_Revenue_Per_Track
FROM
    etl_staging.fact_sales f
JOIN
    etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId 
WHERE
    t.track_author IS NOT NULL
GROUP BY
    t.track_author
ORDER BY
    Total_Revenue DESC;
```

- `genre_yearly_revenue_stats` - poskytuje prehľad o ročných príjmoch podľa hudobných žánrov. Zobrazuje názov žánru, rok a celkové príjmy pre každý žáner v konkrétnom roku:

```sql
CREATE OR REPLACE VIEW analysis.genre_yearly_revenue_stats AS
SELECT
    g.genre_name AS Genre, 
    d.year AS Year, 
    SUM(f.Total) AS Total_Revenue 
FROM
    etl_staging.fact_sales f
JOIN
    etl_staging.dim_tracks t ON f.dim_tracks_trackId = t.trackId 
JOIN
    etl_staging.dim_genres g ON t.dim_genres_genreId = g.genreId 
JOIN
    etl_staging.dim_date d ON f.dateId = d.dateId 
GROUP BY
    g.genre_name, d.year
ORDER BY
    d.year, Total_Revenue DESC;
```

--- 

## 4. Vizualizácia dát

Bolo navrhnutých **5 vizualizácií**:

---

### 1. Štatistiky predaja skladieb podľa žánru

| <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/dashboards/analysis_1.png"> |
|:-:|
| Obrázok 3: Štatistiky predaja skladieb podľa žánru |

Graf zobrazuje predaj skladieb podľa žánru, zoradený od najvyššieho po najnižší počet predajov. Na horizontálnej osi sú celkové predaje, na vertikálnej jednotlivé žánre. Najobľúbenejším žánrom je **Rock** (374,096), nasledovaný **Latin** (177,984) a **Metal** (124,424). Najmenej predajov zaznamenali **Drama** (12,772) a **Reggae** (12,360). Graf nezobrazuje všetky žánre, čo môže znamenať existenciu ďalších kategórií mimo výrezu.

---

### 2. Výkonnosť zamestnancov na základe interakcií s klientmi

| <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/dashboards/analysis_2.png"> |
|:-:|
| Obrázok 4: Výkonnosť zamestnancov na základe interakcií s klientmi |

Graf zobrazuje celkové interakcie zamestnancov s klientmi. Zamestnanci s ID **3, 4 a 5** majú najvyšší počet interakcií, presahujúci **300,000**. Naopak, zamestnanci s ID **1, 2, 6, 7 a 8** nemajú zaznamenané žiadne interakcie. Môže to naznačovať buď ich nízku aktivitu, alebo nedostatok údajov v databáze.

---

### 3. Počet nákupov jednotlivých zákazníkov

| <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/dashboards/analysis_3.png"> |
|:-:|
| Obrázok 5: Počet nákupov jednotlivých zákazníkov |

Graf zobrazuje počet nákupov zákazníkov, kde na horizontálnej osi sú identifikátory zákazníkov (`CUSTOMER_ID`) a na vertikálnej osi celkový počet nákupov (`TOTAL_PURCHASES`). Nákupy zákazníkov sa pohybujú v rozmedzí 10,000 až 25,000, pričom niektorí zákazníci vykazujú vyššiu aktivitu.

---

### 4. Najlepší umelci podľa celkového príjmu

| <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/dashboards/analysis_4.png"> |
|:-:|
| Obrázok 6: Najlepší umelci podľa celkového príjmu |

Graf zobrazuje **najlepších umelcov** zoradených podľa celkového príjmu (**TOTAL_REVENUE**) spolu s počtom predaných skladieb (**TOTAL_TRACKS_SOLD**). Na horizontálnej osi sú uvedené celkové hodnoty príjmov a predajov, pričom žlté stĺpce reprezentujú celkový príjem a modré stĺpce počet predaných skladieb.

---

### 5. Ročné príjmy podľa hudobných žánrov

| <img src="https://github.com/wwtanji/chinook-database_projekt_dt_2024/blob/main/dashboards/analysis_5.png"> |
|:-:|
| Obrázok 7: Ročné príjmy podľa hudobných žánrov |

Tabuľka zobrazuje celkové ročné príjmy rôznych hudobných žánrov od roku **2021** do **2025**, vrátane príjmov pre žánre ako **Alternative**, **Blues**, **Drama** a ďalšie. Výraznejšie zmeny sú viditeľné pri žánroch ako **Alternative & Punk** a **Hip Hop/Rap**.

## Odkazy

- [GitHub repozitár](https://github.com/wwtanji/chinook-database_projekt_dt_2024)
- [Zdrojové datasety](https://www.kaggle.com/datasets/anurag629/chinook-csv-dataset?select=Album.csv)
- [GitHub repozitár Chinook](https://github.com/lerocha/chinook-database?tab=readme-ov-file)
- [Snowflake](https://www.snowflake.com/)

**Autor projektu:** Roman Zotsenko, FPVaI UKF 2024 
