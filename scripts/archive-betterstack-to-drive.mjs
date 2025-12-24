import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { google } from "googleapis";

const {
  BETTERSTACK_CH_URL,      // e.g. https://<region>-connect.betterstackdata.com
  BETTERSTACK_CH_USER,
  BETTERSTACK_CH_PASS,
  BETTERSTACK_LOGS_TABLE,  // e.g. t123456_my_logs
  DRIVE_FOLDER_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON, // full JSON content (recommended)
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
must(GOOGLE_SERVICE_ACCOUNT_JSON, "GOOGLE_SERVICE_ACCOUNT_JSON");

function utcRangeForYesterday() {
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
  const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
  return { start, end };
}

function toCHDateTime64(d) {
  // ClickHouse toDateTime64 string (no trailing Z)
  return d.toISOString().replace("Z", "");
}

async function chQuery(sql) {
  const auth = Buffer.from(`${BETTERSTACK_CH_USER}:${BETTERSTACK_CH_PASS}`).toString("base64");

  const res = await fetch(BETTERSTACK_CH_URL, {
    method: "POST",
    headers: {
      "Authorization": `Basic ${auth}`,
      "Content-Type": "text/plain",
    },
    body: sql,
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`ClickHouse HTTP ${res.status}: ${text.slice(0, 800)}`);
  return text;
}

function loadServiceAccount() {
  const trimmed = GOOGLE_SERVICE_ACCOUNT_JSON.trim();
  return JSON.parse(trimmed);
}

async function uploadToDrive(filePath, fileName) {
  const sa = loadServiceAccount();

  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/drive.file"],
  });

  const drive = google.drive({ version: "v3", auth });

  const created = await drive.files.create({
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

  return created.data;
}

async function main() {
  const { start, end } = utcRangeForYesterday();
  const day = start.toISOString().slice(0, 10);
  const outName = `dixi-logs-${day}.jsonl.gz`;
  const outPath = path.join(os.tmpdir(), outName);

  // Keep it simple: export "dt" + full JSON payload in "message" + syslog fields.
  // Your Better Stack schema may differ; if this errors, we’ll adjust the SELECT fields.
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
