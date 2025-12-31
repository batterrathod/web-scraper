/**
 * Moneyview IVR Scraper
 * NO ENV VARIABLES
 * Direct credentials inside code
 * SAFE for GitHub Actions (one run)
 */

const puppeteer = require("puppeteer");
const mysql = require("mysql2/promise");

/* ===============================
   DIRECT CONFIG (HARDCODED)
================================ */
const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";
const DATA_URL  = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

// 🔴 LOGIN CREDENTIALS
const EMAIL = "admin@switchmyloan.in";
const PASSWORD = "Admin@123";

// 🔴 DATABASE CONFIG
const DB_CONFIG = {
    host: "82.25.121.2",
    user: "u527886566_scraper_db",
    password: "VAKILr6762",
    database: "u527886566_scraper_db",
    port: 3306,
    family: 4,               // ✅ FIXES ::1 / IPv6 ISSUE
    waitForConnections: true,
    connectionLimit: 5
};

/* ===============================
   COLUMN INDEXES
================================ */
const IDX_SN      = 0;
const IDX_NAME    = 1;
const IDX_MSG     = 2;
const IDX_NUMBER  = 3;
const IDX_PAN     = 4;
const IDX_SALARY  = 5;
const IDX_DOB     = 6;
const IDX_CREATED = 7;

/* ===============================
   UTILITIES
================================ */
function log(msg, type = "INFO") {
    console.log(`[${new Date().toISOString()}] [${type}] ${msg}`);
}

function parseDate(val) {
    if (!val) return null;
    val = val.trim();

    const iso = new Date(val);
    if (!isNaN(iso)) return iso.toISOString().split("T")[0];

    const m = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) {
        const d = new Date(m[3], m[2] - 1, m[1]);
        if (!isNaN(d)) return d.toISOString().split("T")[0];
    }
    return null;
}

/* ===============================
   DATABASE INIT
================================ */
async function initDB() {
    log(`Connecting DB → ${DB_CONFIG.host}`);

    const pool = await mysql.createPool(DB_CONFIG);

    await pool.execute(`
        CREATE TABLE IF NOT EXISTS ivr_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sn VARCHAR(50),
            full_name VARCHAR(255),
            moneyview_msg TEXT,
            phone_number VARCHAR(20),
            pan_card VARCHAR(20),
            salary VARCHAR(100),
            dob_raw VARCHAR(50),
            dob DATE,
            created VARCHAR(50),
            scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_record (phone_number, pan_card, created)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    log("Database ready", "SUCCESS");
    return pool;
}

/* ===============================
   BROWSER
================================ */
async function createBrowser() {
    return puppeteer.launch({
        headless: "new",
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage"
        ]
    });
}

/* ===============================
   LOGIN
================================ */
async function login(page) {
    log("Logging in...");
    await page.goto(LOGIN_URL, { waitUntil: "networkidle0", timeout: 60000 });

    await page.type('input[name="email"]', EMAIL, { delay: 40 });
    await page.type('input[name="password"]', PASSWORD, { delay: 40 });

    await Promise.all([
        page.click('button[type="submit"]'),
        page.waitForNavigation({ waitUntil: "networkidle0", timeout: 60000 })
    ]);

    log("Login successful", "SUCCESS");
}

/* ===============================
   SCRAPE
================================ */
async function scrape(page, pool) {
    log("Scraping IVR logs...");
    await page.goto(DATA_URL, { waitUntil: "networkidle0", timeout: 60000 });
    await page.waitForSelector("tbody tr", { timeout: 30000 });

    const rows = await page.evaluate(() =>
        Array.from(document.querySelectorAll("tbody tr")).map(tr =>
            Array.from(tr.querySelectorAll("td")).map(td =>
                td.textContent.replace(/\s+/g, " ").trim()
            )
        )
    );

    log(`Rows found: ${rows.length}`);

    let inserted = 0;
    let duplicates = 0;

    for (const row of rows) {
        if (row.length < 8) continue;

        try {
            await pool.execute(`
                INSERT INTO ivr_logs
                (sn, full_name, moneyview_msg, phone_number, pan_card, salary, dob_raw, dob, created)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE scrape_timestamp = NOW()
            `, [
                row[IDX_SN],
                row[IDX_NAME],
                row[IDX_MSG],
                row[IDX_NUMBER],
                row[IDX_PAN],
                row[IDX_SALARY],
                row[IDX_DOB],
                parseDate(row[IDX_DOB]),
                row[IDX_CREATED]
            ]);

            inserted++;
        } catch (e) {
            if (e.code === "ER_DUP_ENTRY") duplicates++;
            else log(e.message, "ERROR");
        }
    }

    log(`Inserted: ${inserted} | Duplicates: ${duplicates}`, "SUCCESS");
}

/* ===============================
   MAIN (ONE RUN)
================================ */
(async () => {
    try {
        log("Scraper started");

        const pool = await initDB();
        const browser = await createBrowser();
        const page = await browser.newPage();

        await login(page);
        await scrape(page, pool);

        await browser.close();
        await pool.end();

        log("Scraper finished successfully", "SUCCESS");
        process.exit(0);

    } catch (err) {
        log(err.stack || err.message, "CRITICAL");
        process.exit(1);
    }
})();
