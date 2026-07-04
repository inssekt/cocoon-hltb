import pLimit from 'p-limit';
import fetch from 'node-fetch';
import fs from 'fs';
import path from 'path';
import os from 'os';
import process from 'process';

const __dirname = import.meta.dirname;
const outputPath = path.join(__dirname, 'howlongtobeat_games.csv');
const CONCURRENCY = os.cpus().length;
const delay = (ms) => new Promise((res) => setTimeout(res, ms));
let pagesFetched = 0;
let gamesProcessed = 0;
let authToken = "";
let hpKey = "";
let hpVal = "";

const headers = {
  'Referer': 'https://howlongtobeat.com',
  'Origin': 'https://howlongtobeat.com',
  'Content-Type': 'application/json',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0'
};

const baseBody = {
  searchType: "games",
  searchTerms: [""],
  searchPage: 1,
  size: 20,
  searchOptions: {
    games: {
      userId: 0,
      platform: "", // all platforms — "PC" missed every console-exclusive game
      sortCategory: "popular",
      rangeCategory: "main",
      rangeTime: { min: null, max: null },
      gameplay: { perspective: "", flow: "", genre: "", difficulty: "" },
      rangeYear: { min: "", max: "" },
      modifier: ""
    },
    users: { sortCategory: "postcount" },
    lists: { sortCategory: "follows" },
    filter: "",
    sort: 0,
    randomizer: 0
  },
  useCache: true
};

const refreshToken = async() => {
  console.log("🧭 Refreshing auth token...");
  const res = await (await fetch('https://howlongtobeat.com/api/bleed/init?t='+Date.now(), { headers })).json();
  authToken = res.token;
  hpKey = res.hpKey;
  hpVal = res.hpVal;
}

const PAGE_ATTEMPTS = 6; // exponential backoff: ~2+4+8+16+32s rides out transient 503 bursts

const fetchPage = async (pageNum) => {
  const doSearch = async() => {
    // Rebuild the body each attempt: refreshToken() rotates hpKey/hpVal.
    const body = JSON.stringify({ ...baseBody, searchPage: pageNum, [hpKey]: hpVal });
    return await fetch('https://howlongtobeat.com/api/bleed', {
      method: 'POST',
      headers: { ...headers, ...{
        "x-auth-token": authToken,
        "x-hp-key": hpKey,
        "x-hp-val": hpVal,
      }},
      body
    });
  }

  for (let attempt = 0; ; attempt++) {
    try {
      const response = await doSearch();
      if (response.ok) return response.json();
      if (response.status == 403) {
        await refreshToken();
        continue;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (err) {
      if (attempt >= PAGE_ATTEMPTS - 1) throw new Error(`Failed to fetch page ${pageNum}: ${err.message}`);
      await delay(2000 * 2 ** attempt + Math.random() * 1000);
    }
  }
};

const fetchDataFromGameId = async (gameId) => {
  await delay(Math.random() * 100); // 0–100ms delay
  const url = `https://howlongtobeat.com/game/${gameId}`;

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(url, { headers });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const html = await response.text();
      const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">(.+?)<\/script>/);

      if (!match || match.length < 2) throw new Error(`No __NEXT_DATA__ for game ${gameId}`);

      const nextData = JSON.parse(match[1]);
      return nextData?.props?.pageProps?.game?.data?.game[0] || null;

    } catch (err) {
      if (attempt === 2) {
        console.warn(`⚠️ Fetch failed for game ${gameId}: ${err.message}`);
        return null;
      }
      await delay(2000 * 2 ** attempt + Math.random() * 1000);
    }
  }
  return null;
};

const writeRowToCSV = (row, isFirstRow = false) => {
  const line = row.map(cell => `"${String(cell ?? '').replace(/"/g, '""')}"`).join(',') + '\n';
  fs.writeFileSync(outputPath, line, { encoding: 'utf8', flag: isFirstRow ? 'w' : 'a' });
};

(async () => {
  try {
    await refreshToken();
    console.log("🧭 Fetching page 1 to get total page count...");
    const firstPage = await fetchPage(1);
    const totalPages = firstPage.pageTotal;
    const limit = pLimit(CONCURRENCY);

    // Fetch all pages in parallel
    console.log(`🔄 Fetching ${totalPages} pages concurrently...`);
    const pagePromises = [];
    for (let i = 1; i <= totalPages; i++) {
      pagePromises.push(limit(async () => {
        const result = await fetchPage(i);
        pagesFetched++;
        if (pagesFetched % 50 === 0 || pagesFetched === totalPages) {
          console.log(`📦 Fetched ${pagesFetched}/${totalPages} pages`);
        }
        return result;
      }));
    }

    const allPages = await Promise.all(pagePromises);
    const allGames = allPages.flatMap(p => p.data);
    console.log(`🎮 Total games found: ${allGames.length}`);

    // Write CSV header
    writeRowToCSV(["steam_id", "game_name", "comp_main", "comp_plus", "comp_100"], true);

    // Fetch all Steam IDs in parallel
    console.log(`🚀 Fetching Steam IDs using ${CONCURRENCY} threads...`);
    let gamesFailed = 0;
    const gameFetchPromises = allGames.map(game => limit(async () => {
      const gameData = await fetchDataFromGameId(game.game_id);
      if (gameData) {
        const row = [
          gameData.profile_steam,
          gameData.game_name,
          gameData.comp_main,
          gameData.comp_plus,
          gameData.comp_100
        ];
        writeRowToCSV(row, false);
      } else {
        gamesFailed++;
      }
      gamesProcessed++;
      if (gamesProcessed % 100 === 0 || gamesProcessed === allGames.length) {
        console.log(`🎮 Processed ${gamesProcessed}/${allGames.length} games`);
      }
    }));

    await Promise.all(gameFetchPromises);

    // Don't publish a gutted dataset: a small residue of failures is fine, a large one means
    // HLTB was blocking us and the release would silently lose thousands of games.
    if (gamesFailed > allGames.length * 0.05) {
      throw new Error(`Refusing to publish: ${gamesFailed}/${allGames.length} game fetches failed`);
    }
    console.log(`🎉 All done! ${gamesFailed} of ${allGames.length} games failed. CSV saved at ${outputPath}`);

  } catch (error) {
    console.error("❌ Error:", error);
    process.exit(1);
  }
})();
