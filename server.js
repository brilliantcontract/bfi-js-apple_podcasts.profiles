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
  "select url, search_id from apple_podcasts.not_scraped_profiles_vw";
const PROFILE_TABLE_SCHEMA = "apple_podcasts";
const PROFILE_TABLE_NAME = "profiles";
const INSERT_PROFILE_SQL =
  `insert into ${PROFILE_TABLE_SCHEMA}.${PROFILE_TABLE_NAME}(search_id, url, show_name, host_name, show_description, links, reviews, rate, category, episode_description) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`;

/**
 * @typedef {Object} ProfileRequest
 * @property {string} url
 * @property {string | number | null} [searchId]
 */

/**
 * @typedef {Object} Profile
 * @property {string | number | null} searchId
 * @property {string} url
 * @property {string} showName
 * @property {string} hostName
 * @property {string} showDescription
 * @property {string} links
 * @property {string} reviews
 * @property {string} rate
 * @property {string} category
 * @property {string} episodeDescription
 */

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

  const EXCLUDED_DOMAINS = [
    "acast.com",
    "adswizz.com",
    "amazon.com",
    "amzn.to",
    "apple.com",
    "barnesandnoble.com",
    "bookshop.org",
    "books2read.com",
    "buymeacoffee.com",
    "buzzsprout.com",
    "creativecommons.org",
    "megaphone.fm",
    "omnystudio.com",
    "patreon.com",
    "podcastchoices.com",
    "redbubble.com",
    "speaker.com",
    "spotify.com",
  ];

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

  const urlRegex1 = /https?:\/\/[^\s"'◙]+/gi;
  const urlRegex2 = /http?:\/\/[^\s"'◙]+/gi;
  const urlRegex3 = /www\.[^\s"'◙]+/gi;
  const emailRegex = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
  const mentionRegex = /@[A-Za-z0-9_]+/g;
  const urlMatches = [
    ...(description.match(urlRegex1) || []),
    ...(description.match(urlRegex2) || []),
    ...(description.match(urlRegex3) || []),
  ];
  const emailMatches = description.match(emailRegex) || [];
  const mentionMatches = (description.match(mentionRegex) || []).filter(
    (mention) => !emailMatches.some((email) => email.includes(mention))
  );

  const matches = [...urlMatches, ...emailMatches, ...mentionMatches];

  const filtered = matches.filter((value) => {
    if (value.startsWith("@") || value.includes("@")) {
      return true;
    }

    return !skipDomains(value);
  });

  return Array.from(new Set(filtered)).join("◙");
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

function normalizeSearchId(value) {
  if (value === undefined || value === null) {
    return null;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed === "" ? null : trimmed;
  }

  return value;
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

function extractProfileFields(document, url) {
  const showName = cleanText(
    document.querySelector("div.container-detail-header div h1 span")?.textContent || ""
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

  return {
    url: cleanText(url),
    showName,
    hostName,
    showDescription,
    links,
    reviews,
    rate,
    category,
  };
}

function extractEpisodeLinks(document, baseUrl) {
  const rawLinks = Array.from(
    document.querySelectorAll("div.shelf-content > ol > li > div > a[href]")
  )
    .map((link) => link.getAttribute("href") || "")
    .map((link) => link.trim())
    .filter((link) => link !== "");

  return rawLinks
    .map((rawLink) => {
      try {
        return new URL(rawLink, baseUrl).toString();
      } catch (error) {
        return "";
      }
    })
    .filter((link) => link !== "");
}

function extractEpisodeDescription(html) {
  if (!html) {
    return "";
  }

  const dom = new JSDOM(html);
  const document = dom.window.document;

  return (
    document
      .querySelector(
        "div.section.section--paragraph.svelte-1cj8vg9.section--display-separator > div > div"
      )
      ?.innerHTML?.trim() || ""
  );
}

function mergeLinkStrings(...linkStrings) {
  const collected = new Set();

  linkStrings.forEach((value) => {
    if (typeof value !== "string" || value.trim() === "") {
      return;
    }

    value
      .split("◙")
      .map((link) => link.trim())
      .filter((link) => link !== "")
      .forEach((link) => collected.add(link));
  });

  return Array.from(collected).join("◙");
}

function normalizeField(value) {
  const cleaned = cleanText(value);
  return cleaned === "" ? null : cleaned;
}

function validateProfile(profile, url) {
  if (!profile.showName) {
    throw new Error(`Missing show name for profile ${url}`);
  }

  if (!profile.url) {
    throw new Error(`Missing profile URL for profile ${url}`);
  }
}
 
/**
 * @param {import("pg").Pool} pool
 * @returns {Promise<ProfileRequest[]>}
 */
async function loadProfiles(pool) {
  const { rows } = await pool.query(FETCH_PROFILES_SQL);

  return rows
    .map((row) => {
      const url = row && typeof row.url === "string" ? row.url.trim() : "";
      const rawSearchId = row?.search_id ?? row?.searchId;
      const searchId = normalizeSearchId(rawSearchId);

      return { url, searchId };
    })
    .filter(({ url }) => url !== "");
}

/**
 * @param {import("pg").Pool} pool
 * @param {Profile} profile
 * @param {{ insertSql: string }} param2
 */
async function saveProfile(pool, profile, { insertSql }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const values = [
      profile.searchId ?? null,
      normalizeField(profile.url),
      normalizeField(profile.showName),
      normalizeField(profile.hostName),
      normalizeField(profile.showDescription),
      normalizeField(profile.links),
      normalizeField(profile.reviews),
      normalizeField(profile.rate),
      normalizeField(profile.category),
      normalizeField(profile.episodeDescription),
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

/**
 * @param {import("pg").Pool} pool
 * @param {ProfileRequest} profileRequest
 * @param {Record<string, string>} headers
 * @param {{ insertSql: string }} insertConfig
 */
async function processProfile(pool, profileRequest, headers, insertConfig) {
  const html = await fetchProfileHtml(profileRequest.url, headers);
  const dom = new JSDOM(html);
  const document = dom.window.document;
  const extractedProfile = extractProfileFields(document, profileRequest.url);

  const episodeLinks = extractEpisodeLinks(document, profileRequest.url);
  const episodeDescriptions = [];
  const episodeDescriptionLinks = [];

  for (const episodeLink of episodeLinks) {
    try {
      const episodeHtml = await fetchProfileHtml(episodeLink, headers);
      const episodeDescription = extractEpisodeDescription(episodeHtml);
      if (episodeDescription) {
        episodeDescriptions.push(episodeDescription);
        const links = extractLinksFromDescription(episodeDescription);
        if (links) {
          episodeDescriptionLinks.push(links);
        }
      }
    } catch (error) {
      console.error(
        `Failed to fetch episode description from ${episodeLink}: ${error.message}`
      );
    }
  }

  const profile = {
    ...extractedProfile,
    searchId: profileRequest.searchId,
    links: mergeLinkStrings(
      extractedProfile.links,
      episodeDescriptionLinks.join("◙")
    ),
    episodeDescription: episodeDescriptions.join("◙"),
  };

  validateProfile(profile, profileRequest.url);

  await saveProfile(pool, profile, insertConfig);
}

async function main() {
  await ensureDataDir();

  const headerOverrides = await loadHeaderOverrides();
  const headers = buildRequestHeaders(headerOverrides);

  const pool = new Pool(DB_CONFIG);

  try {
    const profiles = await loadProfiles(pool);

    const insertConfig = {
      insertSql: INSERT_PROFILE_SQL,
    };

    if (!profiles.length) {
      console.warn("No profiles found to process.");
      return;
    }

    console.log(
      `Processing ${profiles.length} profile${profiles.length === 1 ? "" : "s"}.`
    );

    for (const profile of profiles) {
      try {
        await processProfile(pool, profile, headers, insertConfig);
        console.log(`Saved profile from ${profile.url}`);
      } catch (error) {
        console.error(
          `Failed to process profile ${profile.url}: ${error.message}`
        );
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
