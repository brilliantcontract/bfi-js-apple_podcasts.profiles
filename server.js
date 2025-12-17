import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { JSDOM } from "jsdom";
import { Pool } from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const HEADERS_FILE = path.join(DATA_DIR, "headers.json");

const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT, 10) || 5432,
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const FETCH_PROFILES_SQL =
  "select url from apple_podcasts.not_scraped_profiles_vw";
const PROFILE_TABLE_SCHEMA = "apple_podcasts";
const PROFILE_TABLE_NAME = "profiles";
const INSERT_PROFILE_SQL =
  `insert into ${PROFILE_TABLE_SCHEMA}.${PROFILE_TABLE_NAME}(show_name, host_name, show_description, links, reviews, rate, category) values ($1, $2, $3, $4, $5, $6, $7)`;

const DEFAULT_HEADERS = {
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "accept-language": "en-US,en;q=0.5",
  "accept-encoding": "deflate",
  connection: "keep-alive",
  cookie: "geo=UA; geo=UA",
  "upgrade-insecure-requests": "1",
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "none",
  "if-none-match": '"I+YQARDtedeoXRsCMXBPLv+zmt8MEws33ZxExP9eEpQ="',
  priority: "u=0, i",
  "user-agent":
    process.env.USER_AGENT ||
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0",
};

const EXCLUDED_DOMAINS = ["patreon.com", "speaker.com"];

function skipDomains(url) {
  if (typeof url !== "string" || url.trim() === "") {
    return false;
  }

  const value = url.trim();
  const normalized = value.match(/^https?:\/\//i) ? value : `https://${value}`;

  try {
    const hostname = new URL(normalized).hostname.toLowerCase();

    return EXCLUDED_DOMAINS.some(
      (domain) => hostname === domain || hostname.endsWith(`.${domain}`)
    );
  } catch (error) {
    return false;
  }
}

async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadHeaderOverrides() {
  try {
    const raw = await fs.readFile(HEADERS_FILE, "utf-8");
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    return parsed;
  } catch (error) {
    return {};
  }
}

function buildRequestHeaders(overrides) {
  const headers = { ...DEFAULT_HEADERS };

  Object.entries(overrides || {}).forEach(([key, value]) => {
    if (typeof value === "string" && value.trim() !== "") {
      headers[key.toLowerCase()] = value.trim();
    }
  });

  return Object.fromEntries(
    Object.entries(headers)
      .map(([key, value]) => [key, value])
      .filter(([, value]) => value !== "")
  );
}

function extractLinksFromDescription(description) {
  if (typeof description !== "string" || description.trim() === "") {
    return "";
  }

  const urlRegex1 = /https?:\/\/[^\s"']+/gi;
  const urlRegex2 = /www\.[^\s"']+/gi;
  const matches = [
    ...(description.match(urlRegex1) || []),
    ...(description.match(urlRegex2) || []),
  ];

  return matches.filter((url) => !skipDomains(url)).join("◙");
}

function shouldUseScrapeNinja() {
  return String(process.env.SCRAPE_NINJA_ENABLED || "").toLowerCase() === "true";
}

function getScrapeNinjaApiKey() {
  return process.env.SCRAPE_NINJA_API_KEY || DEFAULT_SCRAPE_NINJA_API_KEY;
}

async function fetchViaScrapeNinja(url, headers) {
  const apiKey = getScrapeNinjaApiKey();

  if (!apiKey) {
    throw new Error(
      "SCRAPE_NINJA_API_KEY is required when SCRAPE_NINJA_ENABLED is true."
    );
  }

  const response = await fetch(SCRAPE_NINJA_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-rapidapi-key": apiKey,
      "x-rapidapi-host": SCRAPE_NINJA_HOST,
    },
    body: JSON.stringify({
      url,
      method: "GET",
      headers,
      autoparse: false,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `ScrapeNinja request failed with status ${response.status}: ${text.slice(0, 200)}`
    );
  }

  const payload = await response.json();

  if (!payload?.body || typeof payload.body !== "string") {
    throw new Error("ScrapeNinja response did not include a string body.");
  }

  return payload.body;
}

async function fetchProfileHtml(url, headers) {
  if (shouldUseScrapeNinja()) {
    return fetchViaScrapeNinja(url, headers);
  }

  const response = await fetch(url, { method: "GET", headers });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Request failed with status ${response.status}: ${text.slice(0, 200)}`
    );
  }

  return response.text();
}

function cleanText(value) {
  if (typeof value !== "string") {
    return "";
  }

  return value.replace(/\s+/g, " ").trim();
}

function parseReviewsAndRate(metadataText) {
  const cleaned = cleanText(metadataText);

  if (!cleaned) {
    return { reviews: "", rate: "" };
  }

  const parenMatch = cleaned.match(/([0-9]+(?:\.[0-9]+)?)\s*\(([^)]+)\)/);
  if (parenMatch) {
    return { rate: parenMatch[1], reviews: parenMatch[2] };
  }

  const numberMatches = cleaned.match(/([0-9]+(?:\.[0-9]+)?)/g) || [];
  if (numberMatches.length >= 2) {
    return { rate: numberMatches[0], reviews: numberMatches[1] };
  }

  return {
    rate: numberMatches[0] || "",
    reviews: cleaned,
  };
}

function extractProfileFields(html) {
  const dom = new JSDOM(html);
  const document = dom.window.document;

  const showName = cleanText(
    document.querySelector(".headings.svelte-1uuona0 h1")?.textContent || ""
  );
  const hostName = cleanText(
    document.querySelector(".headings__subtitles .svelte-123qhuj")?.textContent ||
      document.querySelector(".headings.svelte-1uuona0 .subtitle-action.svelte-16t2ez2")?.textContent || ""
  );
  const showDescription = cleanText(
    document.querySelector(".description .truncate-wrapper p")?.textContent ||
      document.querySelector(".section.section--paragraph.svelte-1cj8vg9.section--display-separator .shelf-content > div")?.textContent || ""
  );

  const links = extractLinksFromDescription(showDescription);

  const metadataText = cleanText(
    document.querySelector(".metadata.svelte-123qhuj li:nth-child(1)")
      ?.textContent || ""
  );
  const { reviews, rate } = parseReviewsAndRate(metadataText);

  const category = cleanText(
    document.querySelector(".metadata.svelte-123qhuj li:nth-child(2)")
      ?.textContent || ""
  );

  return { showName, hostName, showDescription, links, reviews, rate, category };
}

function normalizeField(value) {
  const cleaned = cleanText(value);
  return cleaned === "" ? null : cleaned;
}

function validateProfile(profile, url) {
  if (!profile.showName) {
    throw new Error(`Missing show name for profile ${url}`);
  }
}

async function loadProfiles(pool) {
  const { rows } = await pool.query(FETCH_PROFILES_SQL);

  return rows
    .map((row) => (row && typeof row.url === "string" ? row.url.trim() : ""))
    .filter((value) => value !== "");
}

async function saveProfile(pool, profile, { insertSql }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const values = [
      normalizeField(profile.showName),
      normalizeField(profile.hostName),
      normalizeField(profile.showDescription),
      normalizeField(profile.links),
      normalizeField(profile.reviews),
      normalizeField(profile.rate),
      normalizeField(profile.category),
    ];

    await client.query(insertSql, values);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function processProfile(pool, url, headers, insertConfig) {
  const html = await fetchProfileHtml(url, headers);
  const profile = extractProfileFields(html);

  validateProfile(profile, url);

  await saveProfile(pool, profile, insertConfig);
}

async function main() {
  await ensureDataDir();

  const headerOverrides = await loadHeaderOverrides();
  const headers = buildRequestHeaders(headerOverrides);

  const pool = new Pool(DB_CONFIG);

  try {
    const urls = await loadProfiles(pool);

    const insertConfig = {
      insertSql: INSERT_PROFILE_SQL,
    };

    if (!urls.length) {
      console.warn("No profiles found to process.");
      return;
    }

    console.log(`Processing ${urls.length} profile${urls.length === 1 ? "" : "s"}.`);

    for (const url of urls) {
      try {
        await processProfile(pool, url, headers, insertConfig);
        console.log(`Saved profile from ${url}`);
      } catch (error) {
        console.error(`Failed to process profile ${url}: ${error.message}`);
      }
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Fatal error while running scraper:", error);
  process.exitCode = 1;
});
