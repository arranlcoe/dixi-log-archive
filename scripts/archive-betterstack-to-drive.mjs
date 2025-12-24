import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { google } from "googleapis";

const {
  BETTERSTACK_CH_URL,      // e.g. https://eu-nbg-2-connect.betterstackdata.com
  BETTERSTACK_CH_USER,
  BETTERSTACK_CH_PASS,
  BETTERSTACK_LOGS_TABLE,  // e.g. t489460_dixi_logs

  DRIVE_FOLDER_ID,

  // OAuth user (required for personal Google Drive uploads)
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

function utcDateTagForYesterday() {
  const now = new Date();
  const today00 = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
  const y = new Date(today00.getTime() - 24 * 60 * 60 * 1000);
  return y.toISOString().slice(0, 10); // YYYY-MM-DD
}

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
  if (!res.ok) throw new Error(`ClickHouse HTTP ${res.status}: ${text.slice(0, 1600)}`);
  return text;
}

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

async function main() {
  const day = utcDateTagForYesterday();
  const outName = `dixi-logs-${day}.jsonl.gz`;
  const outPath = path.join(os.tmpdir(), outName);

  // ClickHouse computes the UTC day boundaries to avoid timestamp formatting issues.
  // OPTIONAL: exclude Render Postgres noise by syslog.appname starting with "dpg-".
  const sql = `
WITH
  toStartOfDay(now('UTC')) AS today_utc,
  today_utc - INTERVAL 1 DAY AS yesterday_utc
SELECT
  dt,
  raw
FROM remote(${BETTERSTACK_LOGS_TABLE})
WHERE dt >= yesterday_utc
  AND dt <  today_utc
  AND JSONExtractString(raw, 'syslog.appname') NOT LIKE 'dpg-%'
ORDER BY dt ASC
FORMAT JSONEachRow
`.trim();

  console.log(`Querying logs for ${day}...`);
  const jsonl = await chQuery(sql);

  if (!jsonl || jsonl.trim().length === 0) {
    console.log(`No logs returned for ${day}. Skipping upload.`);
    return;
  }

  console.log(`Compressing (${jsonl.length} bytes before gzip)...`);
  fs.writeFileSync(outPath, zlib.gzipSync(jsonl, { level: 9 }));

  const size = fs.statSync(outPath).size;
  console.log(`Compressed size: ${size} bytes`);

  console.log("Uploading to Google Drive...");
  const uploaded = await uploadToDrive(outPath, outName);
  console.log("Uploaded:", uploaded);

  fs.unlinkSync(outPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
