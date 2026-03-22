# Bing Webmaster Tools → BigQuery Connector

Google Cloud Function that runs once daily, fetches data from the Bing Webmaster Tools API, and upserts it into BigQuery.

## Tables

| BigQuery table         | Content                                      | Partition field | MERGE key       |
|------------------------|----------------------------------------------|-----------------|-----------------|
| `bing_traffic_daily`   | Aggregate impressions / clicks / CTR         | `date`          | `date`          |
| `bing_pages_daily`     | URL-level traffic (top 100 pages)            | `date`          | `date + url`    |
| `bing_keywords_daily`  | Top search queries (top 100 keywords)        | `snapshot_date` | `query`         |
| `bing_crawl_daily`     | Crawl statistics                             | `date`          | `date`          |

**UPSERT behaviour** — each run uses BigQuery `MERGE` DML, so re-running the function updates existing rows in place instead of inserting duplicates. No streaming buffer involved, fully idempotent.

> **Note on page & keyword limits**: `GetPageStats` and `GetQueryStats` return a maximum of **100 records** each. This is a hard limit in the Bing Webmaster API and cannot be increased.

---

## Environment variables

| Variable             | Required | Description                                         | Default                  |
|----------------------|----------|-----------------------------------------------------|--------------------------|
| `BING_API_KEY`       | ✅        | Bing Webmaster API key                             |                          |
| `BING_SITE_URL`      | ✅        | Site URL exactly as registered in Bing Webmaster   |                          |
| `BQ_PROJECT_ID`      | ✅        | GCP project ID                                     |                          |
| `BQ_DATASET`         | ✅        | BigQuery dataset name                              |                          |
| `BQ_LOCATION`        |           | BigQuery dataset location                          | `EU`                     |
| `BQ_TABLE_TRAFFIC`   |           | Table name for traffic data                        | `bing_traffic_daily`     |
| `BQ_TABLE_PAGES`     |           | Table name for page data                           | `bing_pages_daily`       |
| `BQ_TABLE_KEYWORDS`  |           | Table name for keyword data                        | `bing_keywords_daily`    |
| `BQ_TABLE_CRAWL`     |           | Table name for crawl data                          | `bing_crawl_daily`       |
| `DATA_DELAY_DAYS`    |           | How many days back to fetch (accounts for Bing's data delay) | `3`         |

---

## Bing API endpoints used

| Endpoint                  | Parameters          | Returns                        |
|---------------------------|---------------------|--------------------------------|
| `GetRankAndTrafficStats`  | siteUrl, startDate, endDate | Aggregate traffic per day |
| `GetPageStats`            | siteUrl only        | Top 100 pages                  |
| `GetQueryStats`           | siteUrl only        | Top 100 search queries         |
| `GetCrawlStats`           | siteUrl, startDate, endDate | Crawl stats per day    |

---

## Getting your Bing API key

1. Go to [Bing Webmaster Tools](https://www.bing.com/webmasters/)
2. **Settings** → **API Access** → **Generate API Key**

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
  --memory=256Mi
```

Set environment variables in the Cloud Functions console under **Edit → Variables & Secrets**.

---

## Daily schedule (Cloud Scheduler)

```bash
gcloud scheduler jobs create http bing-webmaster-daily \
  --schedule="0 6 * * *" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/bing-webmaster-ingest" \
  --oidc-service-account-email=YOUR_SA@PROJECT.iam.gserviceaccount.com \
  --time-zone="Europe/Budapest" \
  --location=europe-west1
```

---

## Schema auto-management

Tables are created automatically on first run. If a table exists with an incorrect partition field (e.g. from a previous version), it is **dropped and recreated** automatically with the correct schema. This is safe since all data is re-fetched on each run via MERGE.
