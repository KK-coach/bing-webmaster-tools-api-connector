"use strict";
const axios = require("axios");
const { BigQuery } = require("@google-cloud/bigquery");

// ---------------------------------------------------------------------------
// ENV VARIABLES
// ---------------------------------------------------------------------------
const BING_API_KEY   = process.env.BING_API_KEY;   // Bing Webmaster API key
const BING_SITE_URL  = process.env.BING_SITE_URL;  // e.g. "https://taxually.com/"
const BQ_PROJECT_ID  = process.env.BQ_PROJECT_ID;
const BQ_DATASET     = process.env.BQ_DATASET;
const BQ_LOCATION    = process.env.BQ_LOCATION || "EU";

// Table names
const BQ_TABLE_TRAFFIC  = process.env.BQ_TABLE_TRAFFIC  || "bing_traffic_daily";
const BQ_TABLE_PAGES    = process.env.BQ_TABLE_PAGES    || "bing_pages_daily";
const BQ_TABLE_KEYWORDS = process.env.BQ_TABLE_KEYWORDS || "bing_keywords_daily";
const BQ_TABLE_CRAWL    = process.env.BQ_TABLE_CRAWL    || "bing_crawl_daily";

// Bing data typically has a 3-day delay before the API serves it.
// DATA_DELAY_DAYS shifts the fetch window back accordingly.
// e.g. delay=3, today=March 11 → fetches March 8–10
const DATA_DELAY_DAYS = parseInt(process.env.DATA_DELAY_DAYS || "3", 10);

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------
const REQUIRED_VARS = ["BING_API_KEY", "BING_SITE_URL", "BQ_PROJECT_ID", "BQ_DATASET"];
for (const v of REQUIRED_VARS) {
  if (!process.env[v]) {
    console.error(`❌ Missing required env var: ${v}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Bing Webmaster API
// ---------------------------------------------------------------------------
const BING_BASE    = "https://ssl.bing.com/webmaster/api.svc/json";
const bingHeaders  = () => ({ "Content-Type": "application/json", Accept: "application/json" });
const bingParams   = (extra = {}) => ({ apikey: BING_API_KEY, ...extra });

// ---------------------------------------------------------------------------
// BigQuery client
// ---------------------------------------------------------------------------
const bigquery = new BigQuery({ projectId: BQ_PROJECT_ID });

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------
function isoDateOnly(d) {
  return d.toISOString().slice(0, 10);
}

function nDaysAgo(n) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  return isoDateOnly(d);
}

/**
 * Fetch window adjusted for Bing's data delay.
 * endDate   = yesterday
 * startDate = today - DATA_DELAY_DAYS
 */
function fetchWindow() {
  const endDate   = nDaysAgo(1);
  const startDate = nDaysAgo(DATA_DELAY_DAYS);
  return { startDate, endDate };
}

// ---------------------------------------------------------------------------
// BigQuery schemas
// ---------------------------------------------------------------------------
// NOTE: partition field is now 'date' (actual data date) for traffic/pages/crawl.
//       'snapshot_date' stays as a metadata field (last upserted date).
//       For keywords (no date from API), partition on 'snapshot_date'.

const TRAFFIC_SCHEMA = [
  { name: "date",          type: "DATE"      }, // ← partition field
  { name: "impressions",   type: "INTEGER"   },
  { name: "clicks",        type: "INTEGER"   },
  { name: "ctr",           type: "FLOAT"     },
  { name: "avg_position",  type: "FLOAT"     },
  { name: "snapshot_date", type: "DATE"      }, // last upserted date (metadata)
  { name: "fetched_at",    type: "TIMESTAMP" },
];

const PAGES_SCHEMA = [
  { name: "date",          type: "DATE"      }, // ← partition field
  { name: "url",           type: "STRING"    },
  { name: "impressions",   type: "INTEGER"   },
  { name: "clicks",        type: "INTEGER"   },
  { name: "ctr",           type: "FLOAT"     },
  { name: "avg_position",  type: "FLOAT"     },
  { name: "snapshot_date", type: "DATE"      },
  { name: "fetched_at",    type: "TIMESTAMP" },
];

const KEYWORDS_SCHEMA = [
  { name: "query",         type: "STRING"    }, // MERGE key (no data date from API)
  { name: "impressions",   type: "INTEGER"   },
  { name: "clicks",        type: "INTEGER"   },
  { name: "ctr",           type: "FLOAT"     },
  { name: "avg_position",  type: "FLOAT"     },
  { name: "snapshot_date", type: "DATE"      }, // ← partition field (also MERGE key)
  { name: "fetched_at",    type: "TIMESTAMP" },
];

const CRAWL_SCHEMA = [
  { name: "date",          type: "DATE"      }, // ← partition field
  { name: "crawled",       type: "INTEGER"   },
  { name: "errors",        type: "INTEGER"   },
  { name: "snapshot_date", type: "DATE"      },
  { name: "fetched_at",    type: "TIMESTAMP" },
];

// ---------------------------------------------------------------------------
// BigQuery: ensure dataset + table (auto-recreates if partition field changed)
// ---------------------------------------------------------------------------
async function ensureDatasetExists() {
  const dataset = bigquery.dataset(BQ_DATASET);
  const [exists] = await dataset.exists();
  if (!exists) {
    await dataset.create({ location: BQ_LOCATION });
    console.log(`✅ Dataset created: ${BQ_DATASET}`);
  }
  return dataset;
}

/**
 * Ensures the table exists with the correct schema and partition.
 * If the table exists but has a different partition field, it is dropped and recreated.
 * @param {string}   tableId        BigQuery table ID
 * @param {object[]} schemaFields   Array of { name, type }
 * @param {string}   partitionField Field to partition by (DAY), or null
 */
async function ensureTableExists(tableId, schemaFields, partitionField) {
  const dataset = await ensureDatasetExists();
  const table   = dataset.table(tableId);
  let [exists]  = await table.exists();

  if (exists) {
    const [meta]      = await table.getMetadata();
    const currentPart = meta.timePartitioning?.field ?? null;

    if (currentPart !== (partitionField ?? null)) {
      console.log(
        `⚠️  ${tableId}: partition field mismatch ` +
        `(current: "${currentPart}" → expected: "${partitionField}"). Recreating table...`
      );
      await table.delete();
      exists = false;
    }
  }

  if (!exists) {
    console.log(`🛠️  Creating table ${tableId} (partition: ${partitionField ?? "none"})...`);
    const opts = { schema: { fields: schemaFields } };
    if (partitionField) {
      opts.timePartitioning = { type: "DAY", field: partitionField };
      opts.clustering       = { fields: [partitionField] };
    }
    await dataset.createTable(tableId, opts);
    console.log(`✅ Table created: ${tableId}`);
  }
}

// ---------------------------------------------------------------------------
// BigQuery: MERGE / UPSERT via DML
// ---------------------------------------------------------------------------

/** Format a JS value for inline BigQuery SQL.
 *  NULL values must be explicitly cast — otherwise BigQuery infers
 *  the type as INT64 inside a STRUCT, causing type-mismatch errors
 *  in MERGE ON clauses that compare STRING/DATE columns to INT64. */
function fmtVal(value, type) {
  if (value === null || value === undefined) {
    switch (type) {
      case "DATE":      return "CAST(NULL AS DATE)";
      case "TIMESTAMP": return "CAST(NULL AS TIMESTAMP)";
      case "STRING":    return "CAST(NULL AS STRING)";
      case "INTEGER":   return "CAST(NULL AS INT64)";
      case "FLOAT":     return "CAST(NULL AS FLOAT64)";
      default:          return "NULL";
    }
  }
  switch (type) {
    case "DATE":
      return `DATE '${value}'`;
    case "TIMESTAMP":
      return `TIMESTAMP '${String(value).replace("T", " ").replace("Z", " UTC")}'`;
    case "STRING":
      return `'${String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
    case "INTEGER":
      return String(Number.isFinite(Number(value)) ? Math.round(Number(value)) : 0);
    case "FLOAT":
      return String(Number.isFinite(Number(value)) ? Number(value) : 0);
    default:
      return `'${String(value)}'`;
  }
}

/**
 * Generic UPSERT using BigQuery MERGE DML.
 * No streaming buffer involved → fully idempotent and re-run safe.
 *
 * @param {string}   tableId    BigQuery table ID
 * @param {object[]} rows       Data rows (keys must match schema field names)
 * @param {object[]} schema     Array of { name, type }
 * @param {string[]} mergeKeys  Natural key field names for the ON clause
 */
async function mergeRows(tableId, rows, schema, mergeKeys) {
  if (!rows.length) {
    console.log(`ℹ️  ${tableId}: no rows to upsert.`);
    return;
  }

  // Deduplicate by merge keys — BigQuery MERGE requires unique keys in the source
  const seen = new Map();
  for (const row of rows) {
    const key = mergeKeys.map(k => String(row[k] ?? "")).join("|");
    seen.set(key, row); // last occurrence wins
  }
  const deduped = Array.from(seen.values());
  if (deduped.length < rows.length) {
    console.log(`ℹ️  ${tableId}: deduplicated ${rows.length} → ${deduped.length} rows`);
  }
  rows = deduped;

  const onClause  = mergeKeys.map(k => `T.\`${k}\` = S.\`${k}\``).join(" AND ");
  const updateSet = schema
    .filter(f => !mergeKeys.includes(f.name))
    .map(f => `T.\`${f.name}\` = S.\`${f.name}\``)
    .join(", ");
  const colNames  = schema.map(f => `\`${f.name}\``).join(", ");
  const colVals   = schema.map(f => `S.\`${f.name}\``).join(", ");

  // Process in chunks to stay within BigQuery's 1 MB query size limit
  const CHUNK_SIZE = 400;
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);

    const structs = chunk.map(row => {
      const fields = schema
        .map(f => `${fmtVal(row[f.name], f.type)} AS \`${f.name}\``)
        .join(", ");
      return `STRUCT(${fields})`;
    });

    const sql = `
MERGE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${tableId}\` AS T
USING (
  SELECT * FROM UNNEST([
    ${structs.join(",\n    ")}
  ])
) AS S
ON ${onClause}
WHEN MATCHED THEN
  UPDATE SET ${updateSet}
WHEN NOT MATCHED THEN
  INSERT (${colNames}) VALUES (${colVals})`;

    console.log(`🔄 ${tableId}: merging ${chunk.length} rows (batch ${Math.floor(i / CHUNK_SIZE) + 1})...`);
    await bigquery.query({ query: sql });
  }

  console.log(`✅ ${tableId}: upserted ${rows.length} rows.`);
}

// ---------------------------------------------------------------------------
// Bing API field helpers
// ---------------------------------------------------------------------------

/** Parse Bing WCF date: /Date(ms)/ or /Date(ms±HHMM)/ → "YYYY-MM-DD" */
function parseBingDate(bingDate) {
  if (!bingDate) return null;
  const match = String(bingDate).match(/\/Date\((\d+)([+-]\d+)?\)\//);
  if (!match) return null;
  const ms = parseInt(match[1], 10);
  return isNaN(ms) ? null : isoDateOnly(new Date(ms));
}

function calcCtr(clicks, impressions) {
  if (!impressions || impressions === 0) return 0;
  return parseFloat((clicks / impressions).toFixed(6));
}

// ---------------------------------------------------------------------------
// Bing API fetch functions
// ---------------------------------------------------------------------------

async function fetchTrafficSummary(startDate, endDate) {
  console.log(`📥 GetRankAndTrafficStats: ${startDate} → ${endDate}`);
  const resp = await axios.get(`${BING_BASE}/GetRankAndTrafficStats`, {
    params:  bingParams({ siteUrl: BING_SITE_URL, startDate, endDate }),
    headers: bingHeaders(),
    timeout: 30000,
  });
  const data = resp.data?.d ?? [];
  if (data.length > 0) console.log(`🔍 Traffic raw[0]:`, JSON.stringify(data[0]));
  console.log(`📊 Traffic: ${data.length} records`);
  return data;
}

/**
 * Extract a list from a Bing API response.
 * Bing's JSON API can return the list either as:
 *   { "d": [ ...items... ] }                   → direct array
 *   { "d": { "SomeKey": [ ...items... ] } }    → wrapped in an object
 * We handle both cases.
 */
function extractList(raw, label) {
  if (!raw) {
    console.warn(`⚠️  ${label}: response is null/undefined`);
    return [];
  }

  const d = raw.d;

  // Case 1: d is already an array
  if (Array.isArray(d)) {
    return d;
  }

  // Case 2: d is an object — try common wrapper keys
  if (d && typeof d === "object") {
    const wrapperKeys = [
      "Queries", "QueryStats", "Pages", "PageStats",
      "Results", "Items", "Data", "Value",
    ];
    for (const key of wrapperKeys) {
      if (Array.isArray(d[key])) {
        console.log(`🔍 ${label}: found list under d.${key} (${d[key].length} items)`);
        return d[key];
      }
    }
    // Log the object keys so we can diagnose
    console.warn(`⚠️  ${label}: d is an object but no known array key found. Keys: ${Object.keys(d).join(", ")}`);
    console.warn(`⚠️  ${label}: d value: ${JSON.stringify(d).slice(0, 400)}`);
    return [];
  }

  // Case 3: d is null, 0, or something unexpected
  console.warn(`⚠️  ${label}: unexpected d value (type: ${typeof d}): ${JSON.stringify(d)}`);
  return [];
}

async function fetchPageTraffic() {
  const url = `${BING_BASE}/GetPageStats`;
  console.log(`📥 GetPageStats → ${url}?siteUrl=${BING_SITE_URL}`);
  const resp = await axios.get(url, {
    params:  bingParams({ siteUrl: BING_SITE_URL }),
    headers: bingHeaders(),
    timeout: 30000,
  });
  console.log(`🔍 Pages HTTP status: ${resp.status}`);
  console.log(`🔍 Pages raw response: ${JSON.stringify(resp.data)}`);
  const data = extractList(resp.data, "Pages");
  if (data.length > 0) {
    console.log(`🔍 Pages raw[0] keys: ${Object.keys(data[0]).join(", ")}`);
    console.log(`🔍 Pages raw[0]:`, JSON.stringify(data[0]));
  } else {
    console.log(`ℹ️  Pages: empty — either new site (below threshold) or data not yet available in API`);
  }
  console.log(`📊 Pages: ${data.length} records`);
  return data;
}

async function fetchKeywordStats() {
  // Primary: GetQueryStats (siteUrl only)
  const url = `${BING_BASE}/GetQueryStats`;
  console.log(`📥 GetQueryStats → ${url}?siteUrl=${BING_SITE_URL}`);
  const resp = await axios.get(url, {
    params:  bingParams({ siteUrl: BING_SITE_URL }),
    headers: bingHeaders(),
    timeout: 30000,
  });
  console.log(`🔍 Keywords HTTP status: ${resp.status}`);
  console.log(`🔍 Keywords raw response: ${JSON.stringify(resp.data)}`);
  const data = extractList(resp.data, "Keywords");
  if (data.length > 0) {
    console.log(`🔍 Keywords raw[0]:`, JSON.stringify(data[0]));
  } else {
    console.log(`ℹ️  Keywords: empty — either new site (below threshold) or data not yet available in API`);
  }
  console.log(`📊 Keywords: ${data.length} records`);
  return data;
}

async function fetchCrawlStats(startDate, endDate) {
  console.log(`📥 GetCrawlStats: ${startDate} → ${endDate}`);
  const resp = await axios.get(`${BING_BASE}/GetCrawlStats`, {
    params:  bingParams({ siteUrl: BING_SITE_URL, startDate, endDate }),
    headers: bingHeaders(),
    timeout: 30000,
  });
  const data = resp.data?.d ?? [];
  if (data.length > 0) console.log(`🔍 Crawl raw[0]:`, JSON.stringify(data[0]));
  console.log(`📊 Crawl: ${data.length} records`);
  return data;
}

// ---------------------------------------------------------------------------
// Upsert functions (MERGE into BigQuery)
// ---------------------------------------------------------------------------

async function upsertTraffic(data, snapshotDate, fetchedAt) {
  await ensureTableExists(BQ_TABLE_TRAFFIC, TRAFFIC_SCHEMA, "date");
  if (!data.length) { console.log(`ℹ️  Traffic: no data.`); return; }

  const rows = data.map(r => ({
    date:          parseBingDate(r.Date) ?? snapshotDate,
    impressions:   r.Impressions           ?? 0,
    clicks:        r.Clicks                ?? 0,
    ctr:           calcCtr(r.Clicks, r.Impressions),
    avg_position:  r.AvgImpressionPosition ?? r.AvgPosition ?? 0,
    snapshot_date: snapshotDate,
    fetched_at:    fetchedAt,
  }));

  // MERGE key: date (one row per data date, updated in place)
  await mergeRows(BQ_TABLE_TRAFFIC, rows, TRAFFIC_SCHEMA, ["date"]);
}

async function upsertPages(data, snapshotDate, fetchedAt) {
  await ensureTableExists(BQ_TABLE_PAGES, PAGES_SCHEMA, "date");
  if (!data.length) { console.log(`ℹ️  Pages: no data.`); return; }

  const rows = data.map(r => ({
    date:          parseBingDate(r.Date) ?? snapshotDate,
    url:           r.Query ?? r.Url ?? r.Page ?? null,
    impressions:   r.Impressions           ?? 0,
    clicks:        r.Clicks                ?? 0,
    ctr:           calcCtr(r.Clicks, r.Impressions),
    avg_position:  r.AvgImpressionPosition ?? r.AvgPosition ?? 0,
    snapshot_date: snapshotDate,
    fetched_at:    fetchedAt,
  }));

  // MERGE key: date + url (one row per page per data date)
  await mergeRows(BQ_TABLE_PAGES, rows, PAGES_SCHEMA, ["date", "url"]);
}

async function upsertKeywords(data, snapshotDate, fetchedAt) {
  await ensureTableExists(BQ_TABLE_KEYWORDS, KEYWORDS_SCHEMA, "snapshot_date");
  if (!data.length) { console.log(`ℹ️  Keywords: no data.`); return; }

  const rows = data.map(r => ({
    query:         r.Query ?? r.Keyword ?? null,
    impressions:   r.Impressions           ?? 0,
    clicks:        r.Clicks                ?? 0,
    ctr:           calcCtr(r.Clicks, r.Impressions),
    avg_position:  r.AvgImpressionPosition ?? r.AvgPosition ?? 0,
    snapshot_date: snapshotDate,
    fetched_at:    fetchedAt,
  }));

  // MERGE key: query (GetQueryStats returns no date, so one row per query, updated each run)
  await mergeRows(BQ_TABLE_KEYWORDS, rows, KEYWORDS_SCHEMA, ["query"]);
}

async function upsertCrawl(data, snapshotDate, fetchedAt) {
  await ensureTableExists(BQ_TABLE_CRAWL, CRAWL_SCHEMA, "date");
  if (!data.length) { console.log(`ℹ️  Crawl: no data.`); return; }

  const rows = data.map(r => ({
    date:          parseBingDate(r.Date) ?? snapshotDate,
    crawled:       r.CrawledPages  ?? 0,
    errors:        r.CrawlErrors   ?? 0,
    snapshot_date: snapshotDate,
    fetched_at:    fetchedAt,
  }));

  // MERGE key: date (one row per crawl date)
  await mergeRows(BQ_TABLE_CRAWL, rows, CRAWL_SCHEMA, ["date"]);
}

// ---------------------------------------------------------------------------
// Cloud Function entry point
// ---------------------------------------------------------------------------
let ingestRunning = false;

exports.ingest = async (req, res) => {
  if (ingestRunning) {
    return res.status(409).json({ status: "error", error: "Ingest already running" });
  }
  ingestRunning = true;

  try {
    console.log("🚀 Bing Webmaster ingest started");

    const now          = new Date();
    const snapshotDate = isoDateOnly(now);
    const fetchedAt    = now.toISOString();
    const { startDate, endDate } = fetchWindow();

    console.log(`📅 Period: ${startDate} → ${endDate}  |  delay: ${DATA_DELAY_DAYS}d  |  site: ${BING_SITE_URL}`);

    // Fetch all 4 endpoints in parallel (one failure does not block others)
    const safeCall = async (label, fn) => {
      try {
        return await fn();
      } catch (err) {
        const body = err.response?.data ? JSON.stringify(err.response.data) : "";
        const status = err.response?.status ?? "N/A";
        console.error(`⚠️  Bing API error [${label}] HTTP ${status}: ${err.message} ${body}`);
        return [];
      }
    };

    const [traffic, pages, keywords, crawl] = await Promise.all([
      safeCall("TrafficSummary", () => fetchTrafficSummary(startDate, endDate)),
      safeCall("PageTraffic",    () => fetchPageTraffic()),
      safeCall("TopKeywords",    () => fetchKeywordStats()),
      safeCall("CrawlStats",     () => fetchCrawlStats(startDate, endDate)),
    ]);

    // Upsert sequentially (MERGE is not streaming → no buffer issues)
    await upsertTraffic(traffic,   snapshotDate, fetchedAt);
    await upsertPages(pages,       snapshotDate, fetchedAt);
    await upsertKeywords(keywords, snapshotDate, fetchedAt);
    await upsertCrawl(crawl,       snapshotDate, fetchedAt);

    console.log("✅ Bing Webmaster ingest complete");
    res.status(200).json({
      status: "ok",
      snapshot_date: snapshotDate,
      period: { startDate, endDate },
      counts: {
        traffic:  traffic.length,
        pages:    pages.length,
        keywords: keywords.length,
        crawl:    crawl.length,
      },
    });
  } catch (err) {
    console.error("🔥 Bing ingest error:", err?.message, err?.stack);
    res.status(500).json({ status: "error", error: "Internal server error" });
  } finally {
    ingestRunning = false;
  }
};
