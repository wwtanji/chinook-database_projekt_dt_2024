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
