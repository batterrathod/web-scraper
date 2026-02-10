const puppeteer = require("puppeteer");

const mysql = require("mysql2/promise");

// ================= CONFIG =================

const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";

const DATA_URL = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

const EMAIL = "admin@switchmyloan.in";

const PASSWORD = "Cready@2026";

const REFRESH_INTERVAL = 20000; // 20 sec

// ================= MYSQL POOL =================

const pool = mysql.createPool({

host: "82.25.121.2",

user: "u527886566_credifyy",

password: "VAKILr@6762",

database: "u527886566_credifyy",

waitForConnections: true,

connectionLimit: 5,

queueLimit: 0

});

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ================= DASHBOARD =================

async function getStats() {

try {

    const [totalRows] = await pool.query(

        "SELECT COUNT(*) as total FROM ivr_logs"

    );



    const [todayRows] = await pool.query(

        "SELECT COUNT(*) as today FROM ivr_logs WHERE DATE(captured_at)=CURDATE()"

    );



    return {

        total: totalRows[0].total || 0,

        today: todayRows[0].today || 0

    };



} catch (err) {

    return {

        total: 0,

        today: 0

    };

}

}

function showDashboard(stats, newLeads) {

console.clear();

console.log("====================================");

console.log("        🚀 DATA LEAD BOT");

console.log("====================================");

console.log("📊 Total Leads       :", stats.total);

console.log("📅 Today's Leads     :", stats.today);

console.log("🆕 New This Cycle    :", newLeads);

console.log("⏱ Last Update        :", new Date().toLocaleString());

console.log("====================================");

}

// ================= SAFE INSERT =================

async function insertRows(rows) {

if (!rows || rows.length === 0) return 0;



const now = new Date();



const values = [];



for (const r of rows) {



    if (!r[3]) continue; // skip if number missing



    values.push([

        r[0] || null,

        r[1] || null,

        r[2] || null,

        r[3] || null,

        r[4] || null,

        r[5] || null,

        r[6] || null,

        r[7] || null,

        now

    ]);

}



if (values.length === 0) return 0;



try {



    const [result] = await pool.query(

        `INSERT IGNORE INTO ivr_logs

        (sn, full_name, moneyview_msg, number, pan_card, salary, dob, created, captured_at)

        VALUES ?`,

        [values]

    );



    return result.affectedRows || 0;



} catch (err) {

    console.log("DB Insert Error:", err.message);

    return 0;

}

}

// ================= MAIN BOT =================

process.on("unhandledRejection", err => {

console.error("Unhandled Error:", err.message);

});

(async () => {

const browser = await puppeteer.launch({

    headless: true,

    args: ["--no-sandbox", "--disable-setuid-sandbox"]

});



const page = await browser.newPage();

page.setDefaultTimeout(90000);



async function login() {

    await page.goto(LOGIN_URL, {

        waitUntil: "networkidle2"

    });

    await page.type('input[name="email"]', EMAIL);

    await page.type('input[name="password"]', PASSWORD);



    await Promise.all([

        page.click('button[type="submit"]'),

        page.waitForNavigation({

            waitUntil: "networkidle2"

        })

    ]);



    console.log("✅ Logged in successfully");

}



await login();

await page.goto(DATA_URL, {

    waitUntil: "networkidle2"

});



while (true) {



    try {



        await page.reload({

            waitUntil: "networkidle2"

        });



        // Check if session expired

        if (page.url().includes("login")) {

            console.log("Session expired. Re-logging...");

            await login();

            await page.goto(DATA_URL, {

                waitUntil: "networkidle2"

            });

        }



        await page.waitForSelector("tbody tr", {

            timeout: 30000

        });



        const rows = await page.evaluate(() => {



            const clean = t =>

                t.replace(/\n/g, " ")

                .replace(/[₹,]/g, "")

                .trim();



            return Array.from(document.querySelectorAll("tbody tr"))

                .map(tr =>

                    Array.from(tr.querySelectorAll("td"))

                    .slice(0, 8)

                    .map(td => clean(td.innerText))

                )

                .filter(row => row.length > 0);

        });



        const newLeads = await insertRows(rows);

        const stats = await getStats();



        showDashboard(stats, newLeads);



    } catch (err) {

        console.log("Loop Error:", err.message);

    }



    await sleep(REFRESH_INTERVAL);

}

})();