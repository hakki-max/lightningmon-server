// =============================================================================
// LightningMon Server — NEA Lightning Cache for Pebble Time 2
// Hosted on Render.com (Singapore region)
// =============================================================================
// Architecture:
//   [node-cron every 2 min] --> polls NEA API --> builds rolling 30-min cache
//   [Pebble phone JS]       --> GET /strikes?lat=x&lon=y --> reads cache only
//
// NEA lightning API updates every 2 minutes (data.gov.sg documented cadence).
// Polling faster than 2 minutes returns the same dataset — wasted requests.
// =============================================================================

const express = require('express');
const cron    = require('node-cron');
const https   = require('https');

const app  = express();
const PORT = process.env.PORT || 3000;

// ---------------------------------------------------------------------------
// Configuration constants
// ---------------------------------------------------------------------------

const NEA_API_URL      = 'https://api-open.data.gov.sg/v2/real-time/api/weather?api=lightning';
const CACHE_WINDOW_MS  = 30 * 60 * 1000;  // 30-minute rolling retention window
const DEDUP_RADIUS_KM  = 0.2;             // 200m dedup matches NEA min accuracy
const NEA_POLL_CRON    = '*/2 * * * *';   // Every 2 minutes — matches NEA update cadence

// CAT classification thresholds (km) — aligned to SG ALRAS / MSS context
const CAT1_KM = 5.0;   // Immediate danger
const CAT2_KM = 10.0;  // High risk
const CAT3_KM = 20.0;  // Approaching storm
// Beyond CAT3_KM -> CLEAR

// Singapore regional town reference database
const SG_TOWNS = [
  { name: "Jurong",          lat: 1.3329, lon: 103.7436 },
  { name: "Tuas",            lat: 1.3216, lon: 103.6483 },
  { name: "Choa Chu Kang",   lat: 1.3840, lon: 103.7470 },
  { name: "Woodlands",       lat: 1.4382, lon: 103.7890 },
  { name: "Yishun",          lat: 1.4304, lon: 103.8354 },
  { name: "Ang Mo Kio",      lat: 1.3691, lon: 103.8454 },
  { name: "Sengkang",        lat: 1.3916, lon: 103.8954 },
  { name: "Punggol",         lat: 1.4052, lon: 103.9023 },
  { name: "Pasir Ris",       lat: 1.3721, lon: 103.9474 },
  { name: "Tampines",        lat: 1.3521, lon: 103.9447 },
  { name: "Changi",          lat: 1.3644, lon: 103.9915 },
  { name: "Bedok",           lat: 1.3240, lon: 103.9234 },
  { name: "Marine Parade",   lat: 1.3020, lon: 103.9003 },
  { name: "Downtown / CBD",  lat: 1.2879, lon: 103.8510 },
  { name: "Queenstown",      lat: 1.2942, lon: 103.8060 },
  { name: "Bukit Timah",     lat: 1.3271, lon: 103.8017 },
  { name: "Toa Payoh",       lat: 1.3343, lon: 103.8563 },
  { name: "Serangoon",       lat: 1.3554, lon: 103.8679 },
  { name: "Bishan",          lat: 1.3526, lon: 103.8494 },
  { name: "Clementi",        lat: 1.3162, lon: 103.7649 },
  { name: "Bukit Batok",     lat: 1.3533, lon: 103.7541 },
  { name: "Central Reserve", lat: 1.3678, lon: 103.8122 }
];

// ---------------------------------------------------------------------------
// In-memory strike cache — persists across all Pebble phone requests
// This is the key advantage over the phone-side cache: survives app restarts
// ---------------------------------------------------------------------------
let strikeCache = [];

// Poller state — exposed on /health so you can verify timing on Render dashboard
let lastPollTime    = null;
let lastPollStatus  = 'not yet polled';
let totalPollCount  = 0;
let cacheHitCount   = 0; // how many /strikes requests were served from cache

// ---------------------------------------------------------------------------
// Haversine distance formula (km)
// ---------------------------------------------------------------------------
function calculateDistance(lat1, lon1, lat2, lon2) {
  const R    = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a    =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ---------------------------------------------------------------------------
// NEA API fetch + cache update
// Called by node-cron every 2 minutes AND once immediately on startup
// ---------------------------------------------------------------------------
function pollNEA() {
  console.log(`[${new Date().toISOString()}] Polling NEA API...`);

  https.get(NEA_API_URL, (res) => {
    let raw = '';
    res.on('data', chunk => raw += chunk);
    res.on('end', () => {
      totalPollCount++;
      lastPollTime = new Date().toISOString();

      if (res.statusCode !== 200) {
        lastPollStatus = `HTTP ${res.statusCode}`;
        console.error(`[NEA] Non-200 response: ${res.statusCode}`);
        return;
      }

      try {
        const data = JSON.parse(raw);
        const now  = Date.now();
        let newStrikesAdded = 0;

        // Extract strikes from NEA response structure
        if (
          data && data.data && data.data.records &&
          data.data.records.length > 0 &&
          data.data.records[0].item &&
          data.data.records[0].item.readings
        ) {
          const readings = data.data.records[0].item.readings;

          for (const strike of readings) {
            const lat = parseFloat(
              strike.latitude || (strike.location ? strike.location.latitude : null)
            );
            const lon = parseFloat(
              strike.longitude || (strike.location ? strike.location.longitude : null)
            );

            if (isNaN(lat) || isNaN(lon)) continue;

            // Deduplicate: skip if a near-identical strike is already in cache
            // 200m radius matches NEA's minimum reported location accuracy
            const isDuplicate = strikeCache.some(
              cached => calculateDistance(lat, lon, cached.lat, cached.lon) < DEDUP_RADIUS_KM
            );

            if (!isDuplicate) {
              strikeCache.push({ lat, lon, timestamp: now });
              newStrikesAdded++;
            }
          }
        }

        // Evict entries older than 30-minute rolling window
        const before = strikeCache.length;
        strikeCache   = strikeCache.filter(s => (now - s.timestamp) < CACHE_WINDOW_MS);
        const evicted = before - strikeCache.length + newStrikesAdded;

        lastPollStatus = `OK — +${newStrikesAdded} new, ${strikeCache.length} in cache`;
        console.log(`[NEA] Poll #${totalPollCount}: ${lastPollStatus} (evicted ${evicted} old)`);

      } catch (e) {
        lastPollStatus = `Parse error: ${e.message}`;
        console.error(`[NEA] JSON parse failed: ${e.message}`);
      }
    });
  }).on('error', (e) => {
    lastPollStatus = `Network error: ${e.message}`;
    console.error(`[NEA] Request failed: ${e.message}`);
  });
}

// ---------------------------------------------------------------------------
// CAT classification — pure function, no side effects
// ---------------------------------------------------------------------------
function classifyDistance(km) {
  if (km < CAT1_KM)  return 'CAT 1';
  if (km < CAT2_KM)  return 'CAT 2';
  if (km < CAT3_KM)  return 'CAT 3';
  return 'CLEAR';
}

function nearestTown(lat, lon) {
  let best = { name: 'Unknown Area', dist: 9999 };
  for (const town of SG_TOWNS) {
    const d = calculateDistance(lat, lon, town.lat, town.lon);
    if (d < best.dist) best = { name: town.name, dist: d };
  }
  return best.name;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// GET /strikes?lat=1.3521&lon=103.8198
// Called by Pebble phone JS every 60 seconds — just reads the cache, no NEA call
app.get('/strikes', (req, res) => {
  const userLat = parseFloat(req.query.lat);
  const userLon = parseFloat(req.query.lon);

  if (isNaN(userLat) || isNaN(userLon)) {
    return res.status(400).json({ error: 'Missing or invalid lat/lon parameters' });
  }

  cacheHitCount++;

  if (strikeCache.length === 0) {
    return res.json({
      status:     'CLEAR',
      detail:     'No strikes (30 min)',
      distanceKm: null,
      cacheSize:  0
    });
  }

  // Find the closest strike to the user's GPS position
  let closestKm     = 9999;
  let closestStrike = null;

  for (const strike of strikeCache) {
    const d = calculateDistance(userLat, userLon, strike.lat, strike.lon);
    if (d < closestKm) {
      closestKm     = d;
      closestStrike = strike;
    }
  }

  const cat    = classifyDistance(closestKm);
  const town   = nearestTown(closestStrike.lat, closestStrike.lon);
  const status = cat === 'CLEAR'
    ? `CLEAR (${closestKm.toFixed(1)}km)`
    : `${cat} (${closestKm.toFixed(1)}km)`;

  // Detail string matches what the Pebble C side expects in KEY_DETAIL
  const detail = cat === 'CLEAR'
    ? 'No active threat'
    : `Nr ${town} (+-2km acc)`;

  res.json({
    status,
    detail,
    distanceKm:  parseFloat(closestKm.toFixed(2)),
    cat,
    nearestTown: town,
    cacheSize:   strikeCache.length
  });
});

// GET /health — Render dashboard and manual verification
app.get('/health', (req, res) => {
  res.json({
    ok:             true,
    serverTime:     new Date().toISOString(),
    lastPollTime,
    lastPollStatus,
    totalPolls:     totalPollCount,
    cacheRequests:  cacheHitCount,
    cacheSize:      strikeCache.length,
    cacheWindowMin: CACHE_WINDOW_MS / 60000,
    pollIntervalMin: 2,
    uptime:         `${Math.floor(process.uptime() / 60)} min`
  });
});

// GET / — root, confirms server is alive (also keeps Render awake)
app.get('/', (req, res) => {
  res.send('LightningMon server running. Use /strikes?lat=x&lon=y or /health');
});

// ---------------------------------------------------------------------------
// Startup sequence
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`[LightningMon] Server started on port ${PORT}`);
  console.log(`[LightningMon] Cache window: 30 min | NEA poll: every 2 min`);

  // 1. Immediate first poll so cache is populated before any Pebble request arrives
  console.log('[LightningMon] Running startup pre-fetch...');
  pollNEA();

  // 2. Schedule recurring poll every 2 minutes — matches NEA update cadence exactly
  //    Polling faster returns the same data and wastes Render free-tier resources
  cron.schedule(NEA_POLL_CRON, pollNEA);
  console.log(`[LightningMon] NEA poller scheduled: ${NEA_POLL_CRON}`);
});
