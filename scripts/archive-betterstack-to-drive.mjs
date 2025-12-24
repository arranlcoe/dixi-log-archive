import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { google } from "googleapis";

// ---------- Env ----------
const {
  BETTERSTACK_CH_URL,      // e.g. https://eu-nbg-2-connect.betterstackdata.com
  BETTERSTACK_CH_USER,
  BETTERSTACK_CH_PASS,
  BETTERSTACK_LOGS_TABLE,  // e.g. t489460_dixi_logs
  DRIVE_FOLDER_ID,

  // OAuth (required for personal Google Drive uploads)
  GOOGLE_OAUTH_CLIENT_ID,
  GOOGLE_OAUTH_CLIENT_SECRET,
  GOOGLE_OAUTH_REFRESH_TOKEN,
} = process.env;

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

must(BETTERSTACK_CH_URL, "BETTERSTACK_CH_URL");
must(BETTERSTACK_CH_USER, "BETTERSTACK_CH_USER");
must(BETTERSTACK_CH_PASS, "BETTERSTACK_CH_PASS");
must(BETTERSTACK_LOGS_TABLE, "BETTERSTACK_LOGS_TABLE");
must(DRIVE_FOLDER_ID, "DRIVE_FOLDER_ID");

must(GOOGLE_OAUTH_CLIENT_ID, "GOOGLE_OAUTH_CLIENT_ID");
must(GOOGLE_OAUTH_CLIENT_SECRET, "GOOGLE_OAUTH_CLIENT_SECRET");
must(GOOGLE_OAUTH_REFRESH_TOKEN, "GOOGLE_OAUTH_REFRESH_TOKEN");

// ---------- Time helpers ----------
function utcRangeForYesterday() {
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0)); // today 00:00Z
  const start = new Date(end.getTime() - 24 * 60 * 60 * 1000); // yesterday 00:00Z
  return { start, end };
}

function toCHDateTime64(d) {
  // ClickHouse accepts ISO-ish strings without trailing Z in toDateTime64()
  return d.toISOString().replace("Z", "");
}

// ---------- Better Stack / ClickHouse ----------
async function chQuery(sql) {
  const auth = Buffer.from(`${BETTERSTACK_CH_USER}:${BETTERSTACK_CH_PASS}`).toString("base64");

  const chUrl = BETTERSTACK_CH_URL.trim().replace(/\/+$/, "");
  if (!chUrl.startsWith("http://") && !chUrl.startsWith("https://")) {
    throw new Error(`BETTERSTACK_CH_URL must start with http(s)://. Got: ${chUrl}`);
  }

  const res = await fetch(chUrl, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "text/plain",
    },
    body: sql,
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`ClickHouse HTTP ${res.status}: ${text.slice(0, 1200)}`);
  return text;
}

// ---------- Google Drive upload (OAuth user) ----------
function driveClientFromOAuth() {
  const oauth2 = new google.auth.OAuth2(
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    "https://developers.google.com/oauthplayground"
  );

  oauth2.setCredentials({ refresh_token: GOOGLE_OAUTH_REFRESH_TOKEN });

  return google.drive({ version: "v3", auth: oauth2 });
}

async function uploadToDrive(filePath, fileName) {
  const drive = driveClientFromOAuth();

  const res = await drive.files.create({
    requestBody: {
      name: fileName,
      parents: [DRIVE_FOLDER_ID],
    },
    media: {
      mimeType: "application/gzip",
      body: fs.createReadStream(filePath),
    },
    fields: "id,name",
  });

  return res.data;
}

// ---------- Main ----------
async function main() {
  const { start, end } = utcRangeForYesterday();
  const day = start.toISOString().slice(0, 10); // YYYY-MM-DD
  const outName = `dixi-logs-${day}.jsonl.gz`;
  const outPath = path.join(os.tmpdir(), outName);

  // Export dt + raw JSON blob; parse later if you want
  const sql = `
SELECT
  dt,
  raw
FROM remote(${BETTERSTACK_LOGS_TABLE})
WHERE dt >= toDateTime64('${toCHDateTime64(start)}', 3, 'UTC')
  AND dt <  toDateTime64('${toCHDateTime64(end)}', 3, 'UTC')
ORDER BY dt ASC
FORMAT JSONEachRow
`.trim();

  console.log(`Querying logs for ${day}...`);
  const jsonl = await chQuery(sql);

  console.log("Compressing...");
  fs.writeFileSync(outPath, zlib.gzipSync(jsonl, { level: 9 }));

  console.log("Uploading to Google Drive...");
  const uploaded = await uploadToDrive(outPath, outName);
  console.log("Uploaded:", uploaded);

  fs.unlinkSync(outPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
