/**
 * Moneyview IVR Scraper – SINGLE RUN MODE
 * ✅ GitHub Actions / Cron / Render Compatible
 * ❌ No infinite loop
 */

const puppeteer = require("puppeteer");
const mysql = require("mysql2/promise");

/* ===============================
   CONFIGURATION
================================ */
const DB_CONFIG = {
    host: "82.25.121.2",
    user: "u527886566_scraper_db",
    password: "VAKILr6762",
    database: "u527886566_scraper_db",
    port: 3306,
    waitForConnections: true,
    connectionLimit: 5
};

const LOGIN_CREDENTIALS = {
    email: "admin@switchmyloan.in",
    password: "Admin@123"
};

const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";
const DATA_URL  = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

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
   HELPERS
================================ */
const log = (msg, type = "INFO") =>
    console.log(`[${new Date().toISOString()}] [${type}] ${msg}`);

function parseDate(val) {
    if (!val) return null;
    const iso = new Date(val);
    if (!isNaN(iso)) return iso.toISOString().split("T")[0];

    const m = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) return `${m[3]}-${m[2].padStart(2,"0")}-${m[1].padStart(2,"0")}`;
    return null;
}

/* ===============================
   DATABASE
================================ */
async function initDB() {
    log("Connecting to database...");
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
        )
    `);

    log("Database ready", "SUCCESS");
    return pool;
}

/* ===============================
   BROWSER
================================ */
async function createBrowser() {
    log("Launching browser...");
    return puppeteer.launch({
        headless: "new",
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu"
        ]
    });
}

/* ===============================
   LOGIN
================================ */
async function login(page) {
    log("Logging in...");
    await page.goto(LOGIN_URL, { waitUntil: "networkidle0" });

    await page.type('input[name="email"]', LOGIN_CREDENTIALS.email, { delay: 30 });
    await page.type('input[name="password"]', LOGIN_CREDENTIALS.password, { delay: 30 });

    await Promise.all([
        page.click('button[type="submit"]'),
        page.waitForNavigation({ waitUntil: "networkidle0" })
    ]);

    log("Login successful", "SUCCESS");
}

/* ===============================
   SCRAPE
================================ */
async function scrape(page, pool) {
    log("Opening IVR logs page...");
    await page.goto(DATA_URL, { waitUntil: "networkidle0" });

    await page.waitForSelector("tbody tr");

    const rows = await page.evaluate(() => {
        return Array.from(document.querySelectorAll("tbody tr")).map(tr =>
            Array.from(tr.querySelectorAll("td")).map(td =>
                td.innerText.replace(/\s+/g, " ").trim()
            )
        ).filter(r => r.length >= 8);
    });

    log(`Rows found: ${rows.length}`);

    let inserted = 0, duplicates = 0;

    for (const row of rows) {
        try {
            const [res] = await pool.execute(`
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

            res.affectedRows > 1 ? duplicates++ : inserted++;
        } catch {
            duplicates++;
        }
    }

    log(`Inserted: ${inserted} | Duplicates: ${duplicates}`, "SUCCESS");
}

/* ===============================
   MAIN
================================ */
(async () => {
    let browser, pool;

    try {
        log("Moneyview Scraper Started");

        pool = await initDB();
        browser = await createBrowser();
        const page = await browser.newPage();

        await page.setViewport({ width: 1366, height: 768 });
        await login(page);
        await scrape(page, pool);

        log("Scraping completed successfully", "SUCCESS");
    } catch (err) {
        log(err.message, "ERROR");
        process.exit(1);
    } finally {
        if (browser) await browser.close();
        if (pool) await pool.end();
        log("Clean exit");
    }
})();