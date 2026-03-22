# Bing Webmaster → BigQuery Connector

Google Cloud Function, amely naponta egyszer lekéri a Bing Webmaster Tools adatait és BigQuery-be tölti.

## Adatok

| BigQuery tábla         | Tartalom                          |
|------------------------|-----------------------------------|
| `bing_traffic_daily`   | Összesített impressions / clicks  |
| `bing_pages_daily`     | URL-szintű traffic                |
| `bing_keywords_daily`  | Keresési kifejezések              |
| `bing_crawl_daily`     | Crawl statisztikák                |

Minden tábla **DAY partícionált** `snapshot_date` szerint és idempotens (napi újrafuttatás biztonságos).

---

## Környezeti változók

| Változó              | Kötelező | Leírás                                        |
|----------------------|----------|-----------------------------------------------|
| `BING_API_KEY`       | ✅        | Bing Webmaster API kulcs                     |
| `BING_SITE_URL`      | ✅        | Pl. `https://kk.coach/`                      |
| `BQ_PROJECT_ID`      | ✅        | GCP projekt azonosító                        |
| `BQ_DATASET`         | ✅        | BigQuery dataset neve                        |
| `BQ_LOCATION`        |           | BigQuery lokáció (alapból: `EU`)             |
| `BQ_TABLE_TRAFFIC`   |           | Táblanév (alapból: `bing_traffic_daily`)     |
| `BQ_TABLE_PAGES`     |           | Táblanév (alapból: `bing_pages_daily`)       |
| `BQ_TABLE_KEYWORDS`  |           | Táblanév (alapból: `bing_keywords_daily`)    |
| `BQ_TABLE_CRAWL`     |           | Táblanév (alapból: `bing_crawl_daily`)       |
| `NUM_DAYS`           |           | Lekérendő napok száma (alapból: `1`)         |

---

## Bing API kulcs megszerzése

1. Nyisd meg: https://www.bing.com/webmasters/
2. Settings → API Access → Generate API Key

---

## Deploy (Cloud Functions 2nd gen)

```bash
cd bing-webmaster
npm install

gcloud functions deploy bing-webmaster-ingest \
  --gen2 \
  --runtime=nodejs20 \
  --region=europe-west1 \
  --source=. \
  --entry-point=ingest \
  --trigger-http \
  --no-allow-unauthenticated \
  --timeout=300s \
  --memory=256Mi \
  --set-env-vars="BING_API_KEY=xxx,BING_SITE_URL=https://kk.coach/,BQ_PROJECT_ID=xxx,BQ_DATASET=webmaster"
```

## Napi ütemezés (Cloud Scheduler)

```bash
gcloud scheduler jobs create http bing-webmaster-daily \
  --schedule="0 6 * * *" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/bing-webmaster-ingest" \
  --oidc-service-account-email=YOUR_SA@PROJECT.iam.gserviceaccount.com \
  --time-zone="Europe/Budapest"
```
