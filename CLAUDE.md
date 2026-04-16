# WaterWise — System Invariants

Critical facts that must hold across all code changes. Violating these has caused real bugs.

---

## 1. Data lag — consumptionDate is always MT yesterday

WaterScope always shows the **prior day's** consumption. The scraper runs at ~2am MT and reports Apr 14's data on Apr 15.

`consumptionDate` must be computed as yesterday in MT time, not UTC yesterday:

```js
const MT_OFFSET   = -6; // MDT (UTC-6); safe to use year-round
const mtNow       = new Date(Date.now() + MT_OFFSET * 3600000);
const mtYesterday = new Date(mtNow.getTime() - 24 * 3600000);
const consumptionDate = mtYesterday.toISOString().slice(0, 10);
```

**Why this matters:** At 7pm MT the UTC date is already tomorrow. `new Date().toISOString().slice(0,10)` returns the wrong date. This caused intervals to be stored under Apr 15 containing Apr 14 data.

Interval keys use `consumptionDate` (yesterday), never today:
```
waterwise:{userId}:intervals:{consumptionDate}   ← correct
waterwise:{userId}:intervals:{today}             ← wrong
```

---

## 2. User namespace isolation

Every Redis key must be prefixed correctly:

| Account type | Key prefix |
|---|---|
| Owner (main account) | `waterwise:` |
| Registered user | `waterwise:{userId}:` |

Examples:
```
waterwise:latest                          ← owner
waterwise:172ae020:latest                 ← registered user

waterwise:intervals:2026-04-14            ← owner
waterwise:172ae020:intervals:2026-04-14   ← registered user
```

**Never mix namespaces.** Reading `waterwise:latest` when `userId` is present will show the wrong user's data. Every API endpoint that accepts `?userId=` must namespace all Redis reads.

Pattern used throughout:
```js
const ns = userId ? `waterwise:${userId}` : 'waterwise';
// then: redis.get(`${ns}:latest`)
```

---

## 3. Household profile keys

Owner and user profiles are stored at different keys with different suffixes:

```
waterwise:household:owner        ← owner
waterwise:household:{userId}     ← registered user
```

This does **not** follow the `ns:key` pattern. Always construct explicitly:

```js
const profileKey = userId
  ? `waterwise:household:${userId}`
  : 'waterwise:household:owner';
```

**Why this matters:** Using the owner profile for a registered user leaked bidet and dual-flush toilet settings onto Swiss Shed's dashboard.

---

## 4. Corrections always follow interval saves

After writing interval data to Redis, always run `corrections.js` for the same date and userId. Corrections build `waterwise:{userId}:corrected:{date}` which the API reads for fixture breakdowns and leak alerts.

```js
// With userId:
node scripts/corrections.js 2026-04-14 --userId 172ae020

// Owner:
node scripts/corrections.js 2026-04-14
```

`corrections.js` accepts `--userId` as a flag and namespaces all keys accordingly. Never run corrections without matching the userId of the interval data.

---

## 5. Vercel function limit — max 12 serverless functions

The Hobby plan allows **12 serverless functions**. Adding a new `api/*.js` file will exceed the limit and break deployment.

New endpoints must be consolidated into existing files:
- `/api/user/*` routes → `api/user.js` (URL-based dispatch on `req.url` suffix)
- `/api/household/*` routes → existing `api/household/*.js` files
- New rewrites go in `vercel.json` pointing to an existing function

Current function count is at the limit. Verify before adding any new `api/` file.

---

## 6. Timestamps — Metron local-as-UTC

Metron's `ConsumptionChartDate` field stores **Mountain Time as if it were UTC**. A row timestamped `2026-04-14T06:31:00Z` actually means 6:31am MT, not 6:31am UTC.

Rules:
- Always read timestamps with `.getUTCHours()` / `.getUTCMinutes()` — never `.getHours()`
- Never apply a timezone offset to display times
- Display as-is: `6:31 AM` means 6:31am MT

This affects correction rules (time-of-day checks), event-log matching windows, and the daily timeline display. Using local `.getHours()` would shift every event by 6–7 hours.

---

## 7. Scrape order — owner first, then registered users

`scrape.js` runs in this order:
1. Owner scrape (inline in `main()`) — uses `WATERSCOPE_EMAIL` / `WATERSCOPE_PASSWORD`
2. `scrapeAllUsers()` — iterates `waterwise:creds:*`, decrypts each password, runs `scrapeUser()`

**One failure must not stop the loop.** `scrapeUser()` catches all errors internally and returns `{ success: false, error }`. `scrapeAllUsers()` continues to the next user regardless.

After all users are scraped:
- `waterwise:scrape-health:{date}` is written with per-user results
- `waterwise:scrape-log` rolling list is updated (last 30 runs)
- Failure alert emails are sent for any `success: false` entries
