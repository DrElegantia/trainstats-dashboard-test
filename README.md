# TrainStats Dashboard

Questo repository scarica ogni giorno un CSV da un endpoint HTTP, archivia uno storico incrementale, produce dataset puliti e aggregati e pubblica una dashboard statica interattiva su GitHub Pages.

## Obiettivo dati

Livello bronze
Contiene il CSV sorgente giornaliero compresso e un file meta con data di riferimento, timestamp estrazione e URL sorgente.

Livello silver
Contiene dati normalizzati e tipizzati, con parsing datetime, ritardi numerici, campi stazione normalizzati, chiave univoca deterministica e uno stato corsa computabile.

Livello gold
Contiene aggregazioni pronte per la dashboard. La dashboard carica solo gold, non elabora tutto lo storico nel browser.

## Storage e crescita repository

I CSV grezzi giornalieri possono crescere velocemente. Per mantenere il repository gestibile:
Uso gzip per il bronze e parquet per il silver.
Versiono in git solo gold e l’output sito, e posso decidere di versionare bronze e silver solo per una finestra recente, oppure usare Git LFS per bronze e silver.
Se vuoi conservare tutto lo storico nel repository senza LFS, considera di tenere solo bronze gz e silver parquet per gli ultimi N mesi, e spostare i mesi più vecchi in GitHub Releases come artifact, mantenendo gold completo che è molto più leggero.

La dashboard necessita solo dei file in site/data che sono CSV aggregati, quindi i costi in banda e storage restano contenuti.

## Configurazione senza cambiare codice

config/pipeline.yml contiene:
Soglia puntualità in minuti, usata per definire in orario e in ritardo
Classi istogramma per bucketizzazione del ritardo arrivo
Regole di stato corsa basate su campi testuali e regex
Soglia minima di numerosità per classifiche in dashboard

## Definizioni operative

Ritardi negativi
Un ritardo negativo è anticipo. In gold calcolo minuti_ritardo_tot come somma di max(ritardo,0), minuti_anticipo_tot come somma di max(-ritardo,0) e minuti_netti_tot come differenza.

In orario e in ritardo
In orario significa ritardo arrivo tra 0 e soglia inclusa. In ritardo significa ritardo arrivo maggiore della soglia. Anticipo è ritardo arrivo negativo.

Valori mancanti
I record con datetime non parsabili o chiave mancante sono marcati info_mancante e conteggiati esplicitamente. Le statistiche di quantile su ritardo arrivo ignorano missing.

Stato corsa
Lo stato corsa è derivato da pattern regex configurabili in config/pipeline.yml e viene assegnato con precedenza: cancellato, soppresso, parzialmente cancellato, altrimenti effettuato.

## Esecuzione locale

Crea un virtualenv e installa dipendenze:
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

Run giornaliero, usa ieri in Europe/Rome:
python scripts/run_pipeline.py

Backfill intervallo:
python scripts/run_pipeline.py --start 2026-02-01 --end 2026-02-13

Build solo gold e sito se hai già silver:
python scripts/build_gold.py
python scripts/build_site.py

## Anagrafica stazioni e mappa

stations/stations.csv è versionato e contiene coordinate. La pipeline non fa geocoding online.
stations/update_stations.py scansiona silver e aggiunge in stations/stations_unknown.csv i codici stazione visti ma non presenti in stations.csv.
La mappa in dashboard non si rompe se mancano coordinate, semplicemente non disegna quei marker e mostra una nota.

## Controlli qualità e fallimenti intenzionali

La pipeline fallisce se:
Cambia l’intestazione rispetto a config/schema_expected.json in modalità strict
Mancano colonne chiave
Il parsing datetime fallisce oltre la soglia configurata
Il tasso di chiavi mancanti supera la soglia

Per adattare a piccole variazioni in coda all’header, puoi cambiare schema_expected.json da strict a prefix o required_only.

## GitHub Actions e Pages

Il workflow .github/workflows/daily.yml esegue:
Download giornaliero
Aggiornamento silver incrementale con dedup deterministica
Rigenerazione gold
Copia gold in site/data con manifest
Aggiornamento stations_unknown.csv
Commit automatico dei risultati e deploy su GitHub Pages

Per backfill usa workflow_dispatch e inserisci start_date ed end_date.

## Troubleshooting

Se vedi errore header mismatch:
Controlla schema_expected.json e valuta modalità prefix se la sorgente aggiunge colonne.

Se vedi troppi missing datetime:
Controlla il formato delle colonne Ora partenza programmata e Ora arrivo programmata.
Se la sorgente cambia formato, aggiorna parse_dt_it in scripts/utils.py.

Se la mappa è vuota:
Controlla stations/stations.csv e stations/stations_unknown.csv, aggiungi coordinate mancanti.

Se la dashboard non carica dati:
Verifica che site/data contenga i CSV gold e manifest.json dopo build.
