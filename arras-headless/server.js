(async () => {
    const { Worker } = await import("worker_threads");
    const path = await import("path");
    const { WebSocketServer, WebSocket } = await import("ws");
    const { pack, unpack } = await import("msgpackr");
    const http = await import("http");
    const fs = await import("fs");
    const childProcess = await import("child_process");
    const fetchModule = await import("node-fetch");
    const { HttpsProxyAgent } = await import("https-proxy-agent");
    const realFetch = fetchModule.default || fetchModule;

    const rawLog = console.log.bind(console);
    const noop = () => {};
    console.log = noop;
    console.error = noop;
    console.warn = noop;
    console.info = noop;
    console.debug = noop;

    let totalSpawned = 0;
    let activeBotCount = 0;
    const sessions = new Map();

    const WORKER_MEMORY_MB = parseInt(process.env.ARRAS_WORKER_MEMORY_MB || "128", 10) || 128;
    const BOTS_PER_WORKER = parseInt(process.env.ARRAS_BOTS_PER_WORKER || "40", 10) || 40;
    const PREWARM_POOL_SIZE = parseInt(process.env.ARRAS_PREWARM_POOL || "32", 10) || 32;
    const MAX_PROXIES = parseInt(process.env.ARRAS_MAX_PROXIES || "12000", 10) || 12000;
    const MAX_WORKERS = parseInt(process.env.ARRAS_MAX_WORKERS || "256", 10) || 256;
    const MAX_BOTS_GLOBAL = Math.max(50, parseInt(process.env.ARRAS_MAX_BOTS || "6000", 10) || 6000);
    const PROXY_REFRESH_MS = parseInt(process.env.ARRAS_PROXY_REFRESH_MS || "180000", 10) || 180000;
    // Spawn is sequential (no staggered parallel job) so requested counts
    // fill as fast as proxies/workers allow.
    // Allow proxy-less (direct) spawns as a last resort when the proxy pool is
    // empty. OFF by default: one IP can only hold so many connections before
    // the server flags it. Enable with ARRAS_ALLOW_DIRECT=1.
    const ALLOW_DIRECT = process.env.ARRAS_ALLOW_DIRECT === "1";
    const ARRAS_WS_PROTOCOLS = ["arras.io#v1.4+sls+et0", "arras.io"];

    let PROXY_POOL = [];
    let PROXY_RANKED = [];
    let isFetchingProxies = false;
    let isRankingProxies = false;

    const PROXY_SOURCES = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=8000&country=all&ssl=all&anonymity=all",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://www.proxy-list.download/api/v1/get?type=http",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
        "https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/main/proxies/http.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/im-in-tak/PROXY-LIST/main/proxy.txt",
        "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/gitrecon1455/ProxyScraper/main/proxies.txt",
        "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt",
        "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
        "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/elliottophellia/proxylist/master/results/http/global/http_checked.txt",
        "https://raw.githubusercontent.com/opsxcq/proxy-list/master/list.txt",
        "https://raw.githubusercontent.com/sashkiwer/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/http.txt"
    ];

    async function fetchProxies() {
        if (isFetchingProxies) return;
        isFetchingProxies = true;

        const all = new Set();

        try {
            await Promise.allSettled(
                PROXY_SOURCES.map(async (url) => {
                    try {
                        const res = await realFetch(url, { timeout: 10000 });
                        if (!res.ok) return;

                        const text = await res.text();

                        for (const line of text.split(/\r?\n/)) {
                            const cleaned = line.trim().replace(/^https?:\/\//i, "");

                            if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}$/.test(cleaned)) {
                                all.add(`http://${cleaned}`);

                                if (all.size >= MAX_PROXIES) break;
                            }
                        }
                    } catch {}
                })
            );

            // Only swap the live pool when the fetch actually found something —
            // a bad network moment must not wipe the pool entirely.
            if (all.size) {
                PROXY_POOL = Array.from(all).slice(0, MAX_PROXIES);

                for (let i = PROXY_POOL.length - 1; i > 0; i--) {
                    const j = (Math.random() * (i + 1)) | 0;
                    [PROXY_POOL[i], PROXY_POOL[j]] = [PROXY_POOL[j], PROXY_POOL[i]];
                }

                rawLog(`[proxies] refetched pool (${PROXY_POOL.length})`);
            }
        } finally {
            isFetchingProxies = false;
        }
    }

    async function rankProxies(defenderCount = 0) {
        // Ranking is ONLY for defenders — regular follow bots draw straight
        // from session.proxyQueue and don't care about latency. Skip the entire
        // latency pass unless there are defenders to feed.
        if (!PROXY_POOL.length || isRankingProxies) return;
        if (!(defenderCount > 0)) return;
        isRankingProxies = true;

        try {
            // Only rank as many proxies as the defender spawn actually needs —
            // no reason to latency-test thousands for a handful of defenders.
            const want = Math.min(Math.ceil(defenderCount * 1.5) + 4, PROXY_POOL.length);
            const candidates = PROXY_POOL.slice(0, want);

            const results = await Promise.all(
                candidates.map(async (proxyUrl) => {
                    const start = Date.now();

                    try {
                        const res = await realFetch("https://arras.io", {
                            agent: new HttpsProxyAgent(proxyUrl),
                            timeout: 6000
                        });

                        await res.arrayBuffer();

                        return { proxyUrl, ms: Date.now() - start };
                    } catch {
                        return null;
                    }
                })
            );

            // Fastest first. Only the tested slice goes into the ranked queue;
            // the untested tail of the pool stays untouched for regular bots.
            const ranked = results.filter(Boolean).sort((a, b) => a.ms - b.ms);
            PROXY_RANKED = ranked.map((r) => r.proxyUrl);

            rawLog(`[proxies] ranked ${ranked.length}/${candidates.length} (best ${ranked.length ? ranked[0].ms : "-"}ms)`);
        } finally {
            isRankingProxies = false;
        }
    }

    function resetSessionProxies(session) {
        session.proxyQueue = PROXY_POOL.slice();
    }

    function takeUniqueProxy(session, best = false) {
        // Defenders draw from the latency-ranked head of the pool
        if (best && PROXY_RANKED.length) return PROXY_RANKED.shift();

        if (!session.proxyQueue || session.proxyQueue.length === 0) {
            // Queue ran dry — refill once from the live pool so long-running
            // farms keep spawning instead of dying out.
            if (PROXY_POOL.length) {
                session.proxyQueue = PROXY_POOL.slice();
                return session.proxyQueue.pop();
            }

            // No proxies anywhere right now — direct fallback when enabled.
            if (ALLOW_DIRECT) return "";

            return null;
        }

        return session.proxyQueue.pop();
    }

    let arrasScriptCache = null;
    let arrasWasmCache = null;

    const server = http.createServer((req, res) => {
        res.writeHead(426, { "Content-Type": "text/plain" });
        res.end("lll elk ez big fat noob");
    });

    function randint(a, b) {
        return Math.floor(Math.random() * (b - a + 1)) + a;
    }

    const botWorkerPath = path.join(__dirname, "index.js");

    function extractArrasScript(html) {
        const start = html.indexOf("<script>");

        if (start === -1) throw new Error("no script");

        const s = start + 8;
        const end = html.indexOf("</script", s);

        if (end === -1) throw new Error("no close");

        return html.slice(s, end);
    }

    async function preloadArrasAssets() {
        try {
            const htmlResponse = await realFetch("https://arras.io");

            if (!htmlResponse.ok) {
                throw new Error(
                    `Arras HTML fetch failed: ${htmlResponse.status}`
                );
            }

            const html = await htmlResponse.text();

            rawLog("[preload] fetching https://arras.io/app.wasm ...");
            const wasmResponse = await realFetch("https://arras.io/app.wasm");

            if (!wasmResponse.ok) {
                throw new Error(
                    `Arras WASM fetch failed: ${wasmResponse.status}`
                );
            }

            const wasmBuffer = await wasmResponse.arrayBuffer();

            if (!wasmBuffer.byteLength) {
                throw new Error("Arras WASM response was empty");
            }

            // Copy into a SharedArrayBuffer once. postMessage shares
            // SharedArrayBuffer memory across worker threads by reference
            // instead of structured-cloning a private copy per worker.
            const sharedBuf = new SharedArrayBuffer(wasmBuffer.byteLength);
            new Uint8Array(sharedBuf).set(new Uint8Array(wasmBuffer));

            arrasScriptCache = extractArrasScript(html);
            arrasWasmCache = new Uint8Array(sharedBuf);
            rawLog(`[preload] wasm loaded (${arrasWasmCache.byteLength} bytes, shared)`);
        } catch (err) {
            arrasScriptCache = null;
            arrasWasmCache = null;
            rawLog(`[preload] wasm preload failed: ${err && err.message ? err.message : err}`);
        }
    }

    function createBotWorker(session) {
        const worker = new Worker(botWorkerPath, {
            resourceLimits: {
                maxOldGenerationSizeMb: WORKER_MEMORY_MB,
                maxYoungGenerationSizeMb: 24,
                codeRangeSizeMb: 24
            }
        });

        worker.send = (msg) => worker.postMessage(msg);
        worker.botId = null;
        worker.botIds = [];
        worker.activeBots = 0;
        worker.isPooled = false;
        worker.resolvedHash = null;

        worker.on("error", noop);

        worker.on("message", (message) => {
            if (!message) return;

            if (message.type === "died") {
                const idx = worker.botIds.indexOf(message.id);

                if (idx !== -1) {
                    worker.botIds.splice(idx, 1);
                }

                worker.activeBots = Math.max(0, worker.activeBots - 1);
                activeBotCount = Math.max(0, activeBotCount - 1);
            } else if (message.type === "hash_update" && message.hash) {
                worker.resolvedHash = message.hash;

                if (session) {
                    session.resolvedHash = message.hash;

                    if (session.ws) {
                        try {
                            session.ws.send(pack(["R", message.hash]));
                        } catch {}
                    }
                }
            }
        });

        worker.on("exit", () => {
            let idx = session.workers.indexOf(worker);

            if (idx !== -1) {
                session.workers.splice(idx, 1);
            }

            idx = session.pool.indexOf(worker);

            if (idx !== -1) {
                session.pool.splice(idx, 1);
            }
        });

        return worker;
    }

    function prepareWorker(worker) {
        worker.send({
            type: "prepare",
            arrasCache: arrasScriptCache,
            wasmCache: arrasWasmCache
        });
    }

    function fillPool(session) {
        const total = session.workers.length + session.pool.length;
        const needed = Math.max(0, PREWARM_POOL_SIZE - total);

        for (let i = 0; i < needed; i++) {
            const worker = createBotWorker(session);

            worker.isPooled = true;
            session.pool.push(worker);

            prepareWorker(worker);
        }
    }

    function acquireWorker(session, isDefender) {
        let worker = session.workers.find(
            (w) => w.activeBots < BOTS_PER_WORKER &&
                !!w.isDefender === !!isDefender
        );

        if (worker) return worker;

        if (session.workers.length >= MAX_WORKERS) {
            return session.workers[session.workers.length - 1];
        }

        worker = (!isDefender && session.pool.shift()) || createBotWorker(session);

        worker.isPooled = false;
        worker.isDefender = !!isDefender;

        if (!session.workers.includes(worker)) {
            session.workers.push(worker);
        }

        return worker;
    }

    function spawnBotNow(session, hash, botName, isDefender) {
        // Global farm cap — an "#F 1500" style request breaks out here instead
        // of silently spawning past the target.
        if (activeBotCount >= MAX_BOTS_GLOBAL) return false;

        const proxyUrl = takeUniqueProxy(session, isDefender);

        // null = no proxy available and direct mode disabled. An empty string
        // means a proxy-less (direct) spawn was explicitly allowed.
        if (proxyUrl === null) return false;

        const worker = acquireWorker(session, isDefender);
        const botId = session.nextBotId++;

        worker.botId = botId;
        worker.botIds.push(botId);
        worker.activeBots++;

        let selectedTank = session.tank;

        if (session.tanks.length) {
            selectedTank = session.tanks[session.tankIdx];
            session.tankIdx =
                (session.tankIdx + 1) % session.tanks.length;
        }

        const rawHash = String(hash || "").replace(/^#/, "");

        // Defenders always join the operator's live party (session.resolvedHash
        // / player hash). They intentionally ignore the hash used for normal
        // follow bots (the Ext Server Hash field on F packets).
        // Regular bots use the hash from the F packet.
        let spawnHash;
        if (isDefender) {
            // D packet carries the operator's live location.hash (client ignores
            // Ext Server Hash for defenders). Prefer that so defenders never
            // follow a foreign party used by normal F bots.
            spawnHash = rawHash
                ? "#" + rawHash
                : (session.resolvedHash ? "#" + session.resolvedHash : "#");
        } else {
            // Normal bots: Ext Server Hash / F packet hash
            spawnHash = rawHash
                ? "#" + rawHash
                : (session.resolvedHash ? "#" + session.resolvedHash : "#");
        }

        worker.send({
            type: "start",
            config: {
                id: botId,
                proxy: proxyUrl
                    ? { type: "http", url: proxyUrl }
                    : null,
                hash: spawnHash,
                name: botName,
                stats: [0, 0, 0, 0, 0, 0, 0, 9],
                type: "follow",
                token: "follow-8fe6ca",
                autoFire: false,
                autoRespawn: true,
                keys: [],
                keysHold: [],
                tank: selectedTank,
                chatSpam: "",
                buildOverride: session.botBuild || "",
                initialTarget: {
                    tank: selectedTank,
                    isDefender: !!isDefender,
                    buildOverride: session.botBuild || "",
                    // Seed new spawns with the latest known operator
                    // position so they steer immediately instead of
                    // idling until the next A (position) packet arrives.
                    ...(session.lastA ? {
                        x: session.lastA.payload.x,
                        y: session.lastA.payload.y,
                        mouseX: session.lastA.payload.mouseX,
                        mouseY: session.lastA.payload.mouseY,
                        followMouse: !!session.lastA.payload.mouse,
                        noMove: !!session.lastA.payload.noMove
                    } : {})
                },
                squadId: rawHash,
                reconnectAttempts: 2,
                reconnectDelay: 12000,
                arrasCache: arrasScriptCache,
                wasmCache: arrasWasmCache,
                teamColor: session.teamColor
            }
        });

        // Push the cached position to this worker right away. The bot just
        // spawned with the same coords in initialTarget, and this live
        // update guarantees every bot on the worker (old and new) has a
        // target heading so WASD triggers immediately.
        if (session.lastA) {
            worker.send(
                isDefender
                    ? session.lastA.defenderPayload
                    : session.lastA.payload
            );
        }

        totalSpawned++;
        activeBotCount++;

        return true;
    }

    function spawnBatch(session, hash, botName, count, isDefender) {
        const want = Math.max(1, Math.min(parseInt(count, 10) || 1, 10000));

        if (isDefender && PROXY_RANKED.length === 0) {
            rankProxies(want);
        }

        let spawned = 0;
        let staleMisses = 0;

        for (let i = 0; i < want; i++) {
            if (activeBotCount >= MAX_BOTS_GLOBAL) {
                rawLog(`[spawn] hit global cap ${MAX_BOTS_GLOBAL} — stopped at ${spawned}/${want}`);
                break;
            }

            if (spawnBotNow(session, hash, botName, isDefender)) {
                spawned++;
                staleMisses = 0;
                continue;
            }

            // Proxy miss — try refetch once, then keep going a few times
            staleMisses++;
            if (staleMisses === 1) {
                rawLog(
                    `[spawn] proxy pool low — refetching ` +
                    `(spawned ${spawned}, pool ${PROXY_POOL.length})`
                );
                if (isDefender) {
                    fetchProxies().then(() => rankProxies(want - spawned));
                } else {
                    fetchProxies();
                }
            }

            if (staleMisses >= 8 && PROXY_POOL.length === 0 && !ALLOW_DIRECT) {
                rawLog(`[spawn] no proxies — stopped at ${spawned}/${want}`);
                break;
            }

            // Retry this slot instead of consuming it on a dead proxy miss
            i--;
        }

        rawLog(
            `[spawn] done requested=${want} spawned=${spawned} ` +
            `active=${activeBotCount}/${MAX_BOTS_GLOBAL}`
        );
        return spawned;
    }

    function readTailText(filePath, maxBytes = 1048576) {
        try {
            const stat = fs.statSync(filePath);
            const start = Math.max(0, stat.size - maxBytes);

            const fd = fs.openSync(filePath, "r");
            const buffer = Buffer.alloc(stat.size - start);

            fs.readSync(
                fd,
                buffer,
                0,
                buffer.length,
                start
            );

            fs.closeSync(fd);

            return buffer.toString("utf8");
        } catch {
            return "";
        }
    }

    function getKnownArrasBuildId() {
        if (
            /^[a-f0-9]{16}$/i.test(
                process.env.ARRAS_BUILD_ID || ""
            )
        ) {
            return process.env.ARRAS_BUILD_ID;
        }

        const files = [
            "latest-socket-url.txt",
            "latest-socket-trace.json",
            "socket-resolve-trace.ndjson",
            "last-client-run.log",
            "protocol-only-run.log",
            "capture-browser-session.ndjson",
            "protocol-packets.ndjson"
        ];

        for (const file of files) {
            const text = readTailText(
                path.join(__dirname, file)
            );

            const matches = [
                ...[...text.matchAll(
                    /[?&]b=([a-f0-9]{16})/gi
                )].map((m) => m[1]),

                ...[...text.matchAll(
                    /"b"\s*:\s*"([a-f0-9]{16})"/gi
                )].map((m) => m[1])
            ];

            if (matches.length) {
                return matches[matches.length - 1];
            }
        }

        return "";
    }

    function getBrowserProvenSocketTimestamp(buildId) {
        if (
            /^\d{8,12}$/.test(
                process.env.ARRAS_SOCKET_T || ""
            )
        ) {
            return process.env.ARRAS_SOCKET_T;
        }

        const escaped = String(buildId || "").replace(
            /[.*+?^${}()|[\]\\]/g,
            "\\$&"
        );

        if (!escaped) return "";

        const files = [
            "latest-socket-url.txt",
            "latest-socket-trace.json",
            "socket-resolve-trace.ndjson",
            "last-client-run.log",
            "protocol-only-run.log",
            "capture-browser-session.ndjson",
            "protocol-packets.ndjson"
        ];

        const urlPat = new RegExp(
            `[?&]b=${escaped}(?:&[^\\s"'<>]*)?&t=(\\d{8,12})`,
            "gi"
        );

        const jsonPat = new RegExp(
            `"b"\\s*:\\s*"${escaped}"[\\s\\S]{0,300}?"t"\\s*:\\s*"(\\d{8,12})"`,
            "gi"
        );

        for (const file of files) {
            const text = readTailText(
                path.join(__dirname, file),
                4 * 1048576
            );

            const matches = [
                ...[...text.matchAll(urlPat)].map((m) => m[1]),
                ...[...text.matchAll(jsonPat)].map((m) => m[1])
            ];

            if (matches.length) {
                return matches[matches.length - 1];
            }
        }

        return "";
    }

    async function fetchJsonWithTimeout(
        fetchUrl,
        timeoutMs = 3000
    ) {
        const controller = new AbortController();

        const timer = setTimeout(
            () => controller.abort(),
            timeoutMs
        );

        try {
            const response = await realFetch(fetchUrl, {
                signal: controller.signal
            });

            return await response.json();
        } finally {
            clearTimeout(timer);
        }
    }

    async function probeSocketUrl(
        socketUrl,
        timeoutMs = 2500
    ) {
        return new Promise((resolve, reject) => {
            const socket = new WebSocket(
                socketUrl,
                ARRAS_WS_PROTOCOLS,
                {
                    headers: {
                        "user-agent":
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",

                        "accept-encoding":
                            "gzip, deflate, br, zstd",

                        "accept-language":
                            "en-US,en;q=0.9",

                        "origin":
                            "https://arras.io",

                        "cache-control":
                            "no-cache",

                        "pragma":
                            "no-cache"
                    },

                    origin: "https://arras.io"
                }
            );

            let settled = false;

            const done = (err) => {
                if (settled) return;

                settled = true;
                clearTimeout(timer);

                try {
                    socket.close();
                } catch {}

                err
                    ? reject(err)
                    : resolve();
            };

            const timer = setTimeout(
                () => done(new Error("probe-timeout")),
                timeoutMs
            );

            socket.once("open", () => done());

            socket.once(
                "error",
                (err) => done(
                    err || new Error("probe-error")
                )
            );

            socket.once(
                "close",
                () => done(
                    new Error(
                        "probe-closed-before-open"
                    )
                )
            );
        });
    }

    const DIRECT_STATUS_URLS = [
        "https://ak7oqfc2u4qqcu6i-c.uvwx.xyz:8443/2222/status",
        "https://qrp6ujau11f36bnm-c.uvwx.xyz:8443/2222/status",
        "https://kvn3s3cpcdk4fl6j-c.uvwx.xyz:8443/2222/status"
    ];

    async function resolveSocketUrlDirect(hash) {
        const normalized = String(hash || "")
            .replace(/^#/, "")
            .trim();

        const statusKeys = [normalized];

        const noDigits = normalized.replace(/\d+$/, "");

        if (noDigits && noDigits !== normalized) {
            statusKeys.push(noDigits);
        }

        const buildId = getKnownArrasBuildId();

        if (!buildId) {
            throw new Error("missing-build-id");
        }

        let lastError = null;

        for (const statusUrl of DIRECT_STATUS_URLS) {
            try {
                const statusJson =
                    await fetchJsonWithTimeout(statusUrl);

                let row = null;
                let statusKey = "";

                for (const candidate of statusKeys) {
                    const candidateRow =
                        statusJson?.status?.[candidate];

                    if (
                        candidateRow?.online &&
                        candidateRow.host
                    ) {
                        row = candidateRow;
                        statusKey = candidate;
                        break;
                    }
                }

                if (!row?.online || !row.host) {
                    continue;
                }

                const timestamp =
                    getBrowserProvenSocketTimestamp(buildId);

                if (!timestamp) {
                    throw new Error(
                        "missing-browser-proven-t"
                    );
                }

                const socketUrl =
                    `wss://${row.host}/?a=3&b=${buildId}&t=${timestamp}`;

                await probeSocketUrl(socketUrl);

                return {
                    socketUrl,
                    buildId,
                    statusUrl,
                    timestamp,
                    statusKey
                };
            } catch (err) {
                lastError = err;
            }
        }

        throw lastError ||
            new Error("missing-status-row");
    }


    function launchProtocolOnlyClients(
        session,
        ws,
        hash,
        socketUrl,
        options = {}
    ) {
        const count = Math.max(
            1,
            Math.min(
                parseInt(options.count, 10) || 1,
                50
            )
        );

        const requestedDelay =
            parseInt(options.delay, 10);

        const delay = Math.max(
            0,
            Number.isFinite(requestedDelay)
                ? requestedDelay
                : count > 1
                    ? 500
                    : 0
        );

        const botName =
            options.botName === undefined || options.botName === null
                ? ""
                : String(options.botName).trim();

        const party =
            String(hash || "")
                .replace(/^#/, "")
                .match(/\d+$/)?.[0] || "";

        const scriptPath =
            path.join(
                __dirname,
                "protocol-only-random-client.js"
            );

        // Staggered parallel protocol-only spawn: each child gets its own
        // proxy so dead proxies are skipped instead of shared by all N.
        for (let i = 0; i < count; i++) {
            const timer = setTimeout(() => {
                const proxyUrl = takeUniqueProxy(session);
                if (proxyUrl === null && !ALLOW_DIRECT) {
                    rawLog(
                        `[protocol-spawn] no proxy for child ${i + 1}/${count} — skip`
                    );
                    return;
                }

                const clientLogId =
                    `${hash || "bot"}-${i + 1}`;

                const child =
                    childProcess.spawn(
                        process.execPath,
                        [scriptPath],
                        {
                            cwd: __dirname,

                            env: {
                                ...process.env,
                                ARRAS_SOCKET_URL:
                                    socketUrl,

                                ARRAS_CAPTURE_HASH:
                                    `#${hash}`,

                                ARRAS_BOT_NAME:
                                    botName,

                                ARRAS_PARTY:
                                    party,

                                ARRAS_LOG_U:
                                    "0",

                                ARRAS_CLIENT_LOG_ID:
                                    clientLogId,

                                ARRAS_PROXY_URL:
                                    proxyUrl || ""
                            },

                            stdio: [
                                "ignore",
                                "pipe",
                                "pipe",
                                "ipc"
                            ]
                        }
                    );

                session.protocolClients.add(child);

                child.stdout.on("data", noop);
                child.stderr.on("data", noop);

                child.on(
                    "error",
                    () =>
                        session.protocolClients.delete(child)
                );

                child.on(
                    "exit",
                    () =>
                        session.protocolClients.delete(child)
                );
            }, i * delay);

            session.spawnTimers.add(timer);
        }

        rawLog(
            `[protocol-spawn] parallel queued count=${count} delay=${delay}ms`
        );
    }

    function stopProtocolOnlyClients(session) {
        for (const child of session.protocolClients) {
            try {
                child.kill();
            } catch {}
        }

        session.protocolClients.clear();
    }

    function sendProtocolChild(
        session,
        child,
        message
    ) {
        if (
            !child ||
            !child.connected ||
            child.killed ||
            child.exitCode !== null ||
            child.signalCode !== null
        ) {
            session.protocolClients.delete(child);
            return;
        }

        try {
            child.send(
                message,
                (error) => {
                    if (error) {
                        session.protocolClients.delete(child);
                    }
                }
            );
        } catch {
            session.protocolClients.delete(child);
        }
    }

    async function resolveSocketUrlOnly(
        session,
        ws,
        hash,
        options = {}
    ) {
        const normalized =
            String(hash || "")
                .replace(/^#/, "")
                .trim();

        if (!normalized) {
            if (ws.readyState === 1) {
                ws.send(
                    pack([
                        options.launchProtocol
                            ? "P"
                            : "U",
                        "",
                        "",
                        "missing-hash"
                    ])
                );
            }

            return;
        }

        try {
            const direct =
                await resolveSocketUrlDirect(normalized);

            if (options.launchProtocol) {
                launchProtocolOnlyClients(
                    session,
                    ws,
                    normalized,
                    direct.socketUrl,
                    options
                );
            }

            if (ws.readyState === 1) {
                ws.send(
                    pack([
                        options.launchProtocol
                            ? "P"
                            : "U",
                        normalized,
                        direct.socketUrl,
                        null
                    ])
                );
            }

            return;
        } catch {}

        try {
            fs.rmSync(
                path.join(
                    __dirname,
                    "latest-socket-url.txt"
                ),
                { force: true }
            );
        } catch {}

        const worker =
            createBotWorker(session);

        worker.resolveRequest = {
            ws,
            hash: normalized,
            launchProtocol:
                Boolean(options.launchProtocol),
            count: options.count,
            botName: options.botName,
            delay: options.delay
        };

        session.workers.push(worker);

        worker.send({
            type: "start",

            config: {
                id: `resolve-${Date.now()}`,

                proxy: {
                    type: "http",
                    url:
                        takeUniqueProxy(session) || ""
                },

                hash: "#" + normalized,

                name: "resolver",

                stats: [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    9
                ],

                type: "manual",

                token: "resolve-url",

                autoFire: false,
                autoRespawn: false,

                keys: [],
                keysHold: [],

                tank: "Basic",

                chatSpam: "",

                initialTarget: {
                    tank:
                        session.tank || "basic"
                },

                squadId: normalized,

                reconnectAttempts: 0,

                reconnectDelay: 8000,

                arrasCache:
                    arrasScriptCache,

                wasmCache:
                    arrasWasmCache
            }
        });
    }

    const wss =
        new WebSocketServer({ server });

    wss.on(
        "connection",
        (ws, req) => {
            const addr =
                req.socket.remoteAddress;

            if (!sessions.has(addr)) {
                sessions.set(addr, {
                    workers: [],
                    pool: [],
                    protocolClients:
                        new Set(),

                    spawnTimers:
                        new Set(),

                    // Active parallel spawn job ({ cancel() }) or null
                    spawnJob: null,

                    nextBotId: 0,

                    tank: "auto6",

                    tanks: [],

                    tankIdx: 0,

                    proxyQueue: [],

                    resolvedHash: null,
                    botBuild: "",

                    lastA: null,

                    teamColor: null,

                    ws: null
                });
            }

            const session =
                sessions.get(addr);

            session.ws = ws;

            let challenge = null;
            let verified = false;

            const packet = (...args) => {
                try {
                    ws.send(pack(args));
                } catch {}
            };

            const close = () => {
                try {
                    ws.close();
                } catch {}
            };

            ws.on(
                "message",
                (msg) => {
                    try {
                        const data =
                            unpack(msg);

                        const type =
                            data.shift();

                        switch (type) {
                            case "M":
                                if (
                                    challenge ||
                                    data[0] != 72011
                                ) {
                                    return close();
                                }

                                challenge =
                                    randint(
                                        0b1000000000,
                                        0b1111111111
                                    );

                                packet(
                                    "M",
                                    challenge
                                );

                                break;

                            case "C":
                                if (
                                    data[0] ==
                                    (challenge ^ 845)
                                ) {
                                    verified = true;

                                    resetSessionProxies(
                                        session
                                    );

                                    fillPool(
                                        session
                                    );
                                } else {
                                    close();
                                }

                                break;

                            case "Z":
                                session.tank =
                                    data[0];

                                if (
                                    Array.isArray(
                                        session.tank
                                    )
                                ) {
                                    session.tanks =
                                        session.tank;

                                    session.tankIdx = 0;

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        for (
                                            const id
                                            of w.botIds
                                        ) {
                                            const t =
                                                session.tanks[
                                                    session.tankIdx
                                                ];

                                            w.send({
                                                type:
                                                    "tankselect",
                                                tank: t,
                                                botId: id
                                            });

                                            session.tankIdx =
                                                (
                                                    session.tankIdx +
                                                    1
                                                ) %
                                                session.tanks.length;
                                        }
                                    }

                                    let pIdx = 0;

                                    for (
                                        const child
                                        of session.protocolClients
                                    ) {
                                        const t =
                                            session.tanks[
                                                pIdx
                                            ];

                                        sendProtocolChild(
                                            session,
                                            child,
                                            {
                                                type:
                                                    "tankselect",
                                                tank: t
                                            }
                                        );

                                        pIdx =
                                            (
                                                pIdx + 1
                                            ) %
                                            session.tanks.length;
                                    }
                                } else {
                                    session.tanks = [];

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        w.send({
                                            type:
                                                "tankselect",
                                            tank:
                                                session.tank
                                        });
                                    }

                                    for (
                                        const child
                                        of session.protocolClients
                                    ) {
                                        sendProtocolChild(
                                            session,
                                            child,
                                            {
                                                type:
                                                    "tankselect",
                                                tank:
                                                    session.tank
                                            }
                                        );
                                    }
                                }

                                break;

                            case "F":
                                if (!verified) break;

                                {
                                    const hash =
                                        data[0];

                                    let count = 1;

                                    let botName = "";

                                    const a =
                                        data[1];

                                    const b =
                                        data[2];

                                    if (
                                        typeof a ===
                                            "number" ||
                                        (
                                            typeof a ===
                                            "string" &&
                                            /^\d+$/.test(
                                                String(a)
                                            )
                                        )
                                    ) {
                                        count =
                                            Math.max(
                                                1,
                                                parseInt(
                                                    a,
                                                    10
                                                ) || 1
                                            );

                                        botName =
                                            b === undefined || b === null
                                                ? ""
                                                : String(b).trim();
                                    } else if (
                                        typeof b ===
                                            "number" ||
                                        (
                                            typeof b ===
                                            "string" &&
                                            /^\d+$/.test(
                                                String(b)
                                            )
                                        )
                                    ) {
                                        botName =
                                            a === undefined || a === null
                                                ? ""
                                                : String(a).trim();

                                        count =
                                            Math.max(
                                                1,
                                                parseInt(
                                                    b,
                                                    10
                                                ) || 1
                                            );
                                    } else {
                                        // Name-only / ambiguous form — empty allowed (unnamed)
                                        const raw = a ?? b;
                                        botName =
                                            raw === undefined || raw === null
                                                ? ""
                                                : String(raw).trim();

                                        count = 1;
                                    }

                                    count =
                                        Math.min(
                                            count,
                                            10000
                                        );

                                    // Optional build on F: data[3] = "0/4/3/8/8/9/7/4"
                                    {
                                        const buildRaw = data[3];
                                        if (buildRaw !== undefined && buildRaw !== null) {
                                            const build = String(buildRaw).trim();
                                            if (!build || /^[0-9]+(\/[0-9]+)*$/.test(build)) {
                                                session.botBuild = build;
                                            }
                                        }
                                    }

                                    spawnBatch(
                                        session,
                                        hash,
                                        botName,
                                        count,
                                        false
                                    );
                                }

                                break;

                            case "D":
                                // Defender: spawn N bots rotating through
                                // a fixed tank set (octo -> gale ->
                                // automingler -> repeat). Always follows
                                // the player directly (see "A" handler).
                                if (!verified) break;

                                {
                                    const hash =
                                        data[0];

                                    const count =
                                        Math.min(
                                            Math.max(
                                                1,
                                                parseInt(
                                                    data[1],
                                                    10
                                                ) || 1
                                            ),
                                            10000
                                        );

                                    const botName =
                                        data[2] === undefined || data[2] === null
                                            ? ""
                                            : String(data[2]).trim();

                                    session.tanks = [
                                        "octo",
                                        "gale",
                                        "automingler"
                                    ];

                                    session.tankIdx = 0;

                                                                        spawnBatch(
                                        session,
                                        hash,
                                        botName,
                                        count,
                                        true
                                    );
                                }

                                break;

                            case "U":
                                if (!verified) break;

                                resolveSocketUrlOnly(
                                    session,
                                    ws,
                                    data[0]
                                );

                                break;

                            case "P":
                                if (!verified) break;

                                {
                                    const hash =
                                        data[0];

                                    const count =
                                        Math.max(
                                            1,
                                            parseInt(
                                                data[1],
                                                10
                                            ) || 1
                                        );

                                    const botName =
                                        data[2] === undefined || data[2] === null
                                            ? ""
                                            : String(data[2]).trim();

                                    const requestedDelay =
                                        parseInt(
                                            data[3],
                                            10
                                        );

                                    const options = {
                                        launchProtocol:
                                            true,
                                        count,
                                        botName
                                    };

                                    if (
                                        Number.isFinite(
                                            requestedDelay
                                        ) &&
                                        requestedDelay > 0
                                    ) {
                                        options.delay =
                                            requestedDelay;
                                    }

                                    resolveSocketUrlOnly(
                                        session,
                                        ws,
                                        hash,
                                        options
                                    );
                                }

                                break;

                            case "B":
                                if (!verified) break;

                                for (
                                    const w
                                    of session.workers
                                ) {
                                    // Force-terminate immediately instead of
                                    // waiting for the worker to receive and
                                    // process a "destroy" message on its own
                                    // schedule — this is a hard, synchronous
                                    // kill of the whole worker thread.
                                    try {
                                        w.terminate();
                                    } catch {}

                                    w.botIds = [];
                                    w.activeBots = 0;
                                }

                                session.workers = [];

                                for (
                                    const timer
                                    of session.spawnTimers
                                ) {
                                    clearTimeout(timer);
                                }

                                session.spawnTimers.clear();

                                if (session.spawnJob) {
                                    try { session.spawnJob.cancel(); } catch {}
                                    session.spawnJob = null;
                                }

                                stopProtocolOnlyClients(
                                    session
                                );

                                totalSpawned = 0;

                                // Recompute the live count from the sessions
                                // that kept running — the terminated workers
                                // above are already gone, so a nuke can't leave
                                // an inflated count blocking new spawns.
                                activeBotCount = 0;
                                for (const s of sessions.values()) {
                                    for (const w of s.workers) {
                                        activeBotCount += w.activeBots;
                                    }
                                    for (const w of s.pool) {
                                        activeBotCount += w.activeBots;
                                    }
                                }

                                resetSessionProxies(
                                    session
                                );

                                fillPool(
                                    session
                                );

                                break;

                            case "A":
                                if (!verified) break;

                                {
                                    const payload = {
                                        type:
                                            "position",

                                        x: data[0],
                                        y: data[1],

                                        mouseX:
                                            data[2],

                                        mouseY:
                                            data[3],

                                        mouseDown:
                                            data[4],

                                        rMouseDown:
                                            data[5],

                                        mouse:
                                            data[6],

                                        feeding:
                                            data[7]
                                                ? 1
                                                : 0,

                                        shift:
                                            data[8],

                                        autofire:
                                            data[9]
                                                ? 1
                                                : 0,

                                        autospin:
                                            data[10]
                                                ? 1
                                                : 0,

                                        manualMode:
                                            data[11],

                                        manualX:
                                            data[12],

                                        manualY:
                                            data[13],

                                        noMove:
                                            data[14]
                                                ? 1
                                                : 0,

                                        // R override (same pattern as E autofire @ data[9])
                                        override:
                                            data[15]
                                                ? 1
                                                : 0,

                                        wavy:
                                            data[16] === undefined
                                                ? undefined
                                                : data[16]
                                                    ? 1
                                                    : 0,
                                        wavyAmp: data[17],
                                        wavyFreq: data[18],
                                        // Match leader aim direction on each bot
                                        copyAim:
                                            data[19]
                                                ? 1
                                                : 0,

                                        teamColor:
                                            session.teamColor
                                    };

                                    // Defender bots always chase the
                                    // player's real x/y directly — never
                                    // manual coords, never noMove, never
                                    // whatever formation offset was baked
                                    // into the regular payload's x/y.
                                    const defenderPayload = {
                                        type:
                                            "position",

                                        x: data[0],
                                        y: data[1],

                                        mouseX:
                                            data[2],

                                        mouseY:
                                            data[3],

                                        mouseDown:
                                            data[4],

                                        rMouseDown:
                                            data[5],

                                        mouse: true,

                                        feeding: 0,

                                        shift:
                                            data[8],

                                        autofire:
                                            data[9]
                                                ? 1
                                                : 0,

                                        autospin:
                                            data[10]
                                                ? 1
                                                : 0,

                                        override:
                                            data[15]
                                                ? 1
                                                : 0,

                                        copyAim:
                                            data[19]
                                                ? 1
                                                : 0,

                                        manualMode: false,
                                        manualX: 0,
                                        manualY: 0,

                                        noMove: false,

                                        teamColor:
                                            session.teamColor
                                    };

                                    // Remember the latest validated position
                                    // packet so brand-new bots can be seeded
                                    // with it on spawn (no more idle "waiting
                                    // for WASD" until the next A packet).
                                    session.lastA = {
                                        payload,
                                        defenderPayload
                                    };

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        w.send(
                                            w.isDefender
                                                ? defenderPayload
                                                : payload
                                        );
                                    }

                                    for (
                                        const child
                                        of session.protocolClients
                                    ) {
                                        sendProtocolChild(
                                            session,
                                            child,
                                            payload
                                        );
                                    }
                                }

                                break;

                            case "T":
                                if (!verified) break;

                                {
                                    const payload = {
                                        type: "chat",
                                        message: data[0],
                                        spam: data[1]
                                    };

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        w.send(
                                            payload
                                        );
                                    }
                                }

                                break;

                            case "H":
                                if (!verified) break;

                                {
                                    const team =
                                        String(
                                            data[0] || ""
                                        )
                                            .toLowerCase()
                                            .trim();

                                    if (
                                        [
                                            "green",
                                            "blue",
                                            "pink",
                                            "purple"
                                        ].includes(
                                            team
                                        ) &&
                                        session.teamColor !==
                                            team
                                    ) {
                                        session.teamColor =
                                            team;

                                        for (
                                            const w
                                            of session.workers
                                        ) {
                                            w.send({
                                                type:
                                                    "teamcolor",
                                                teamColor:
                                                    team
                                            });
                                        }
                                    }
                                }

                                break;

                            case "G":
                                if (!verified) break;

                                {
                                    const huntName =
                                        String(
                                            data[0] ??
                                            ""
                                        ).trim();

                                    const huntCount =
                                        parseInt(
                                            data[1],
                                            10
                                        ) || 0;

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        w.send({
                                            type:
                                                "huntname",
                                            name:
                                                huntName,
                                            count:
                                                huntCount
                                        });
                                    }
                                }

                                break;

                            case "S":
                                if (!verified) break;

                                {
                                    const enabled =
                                        data[0];

                                    for (
                                        const w
                                        of session.workers
                                    ) {
                                        w.send({
                                            type:
                                                "sing",
                                            enabled:
                                                !!enabled
                                        });
                                    }
                                }

                                break;

                            default:
                                break;
                        }
                    } catch {}
                }
            );

            ws.on(
                "close",
                () => {
                    for (
                        const w
                        of session.workers
                    ) {
                        try {
                            w.terminate();
                        } catch {}
                    }

                    session.workers = [];
                    session.pool = [];

                    stopProtocolOnlyClients(
                        session
                    );

                    for (
                        const timer
                        of session.spawnTimers
                    ) {
                        clearTimeout(timer);
                    }

                    session.spawnTimers.clear();

                    if (session.spawnJob) {
                        try { session.spawnJob.cancel(); } catch {}
                        session.spawnJob = null;
                    }

                    sessions.delete(addr);

                    // Recompute the global count from the sessions that
                    // survived the disconnect.
                    activeBotCount = 0;
                    for (const s of sessions.values()) {
                        for (const w of s.workers) {
                            activeBotCount += w.activeBots;
                        }
                        for (const w of s.pool) {
                            activeBotCount += w.activeBots;
                        }
                    }
                }
            );

            ws.on("error", noop);
        }
    );

    // Fire-and-forget — server listens immediately, caches fill in async.
    // Workers self-load arras.io assets when the caches are still null.
    fetchProxies();
    preloadArrasAssets();

    // Keep the proxy pool fresh — 1500 bots burn through proxies on every
    // reconnect/respawn, so a one-shot fetch starves after a few minutes.
    // No rankProxies here: ranking is defender-only and gets kicked from
    // the defender spawn path when it's actually needed.
    setInterval(() => {
        fetchProxies();
    }, PROXY_REFRESH_MS);

    // Cheap liveness telemetry so long runs can be observed.
    setInterval(() => {
        let workerCount = 0;
        for (const s of sessions.values()) {
            workerCount += s.workers.length + s.pool.length;
        }
        rawLog(`[stats] bots=${activeBotCount}/${MAX_BOTS_GLOBAL} spawned=${totalSpawned} workers=${workerCount} proxies=${PROXY_POOL.length}/${MAX_PROXIES} ranked=${PROXY_RANKED.length}`);
    }, 30000).unref();

    function shutdown(signal) {
        rawLog(`[server] ${signal} — shutting down ${sessions.size} session(s)`);

        for (const session of sessions.values()) {
            for (const w of session.workers) {
                try { w.terminate(); } catch {}
            }
            for (const w of session.pool) {
                try { w.terminate(); } catch {}
            }
            stopProtocolOnlyClients(session);
            for (const timer of session.spawnTimers) {
                clearTimeout(timer);
            }
            if (session.spawnJob) {
                try { session.spawnJob.cancel(); } catch {}
                session.spawnJob = null;
            }
        }

        try { wss.close(); } catch {}
        try { server.close(); } catch {}
        process.exit(0);
    }

    process.once("SIGINT", () => shutdown("SIGINT"));
    process.once("SIGTERM", () => shutdown("SIGTERM"));

    const port =
        process.env.PORT || 8082;

    server.listen(port, async () => {
        const codespaceName = process.env.CODESPACE_NAME;
        const domain = process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN || "app.github.dev";

        const url = codespaceName
            ? `https://${codespaceName}-${port}.${domain}/`
            : `http://localhost:${port}/`;

        rawLog(`[server] url: ${url}`);

        try {
            const qrcodeModule = await import("qrcode-terminal");
            const qrcode = qrcodeModule.default || qrcodeModule;
            qrcode.generate(url, { small: true }, (qr) => rawLog(qr));
        } catch (err) {
            rawLog(`[server] qrcode-terminal not installed — run: npm install qrcode-terminal`);
        }
    });
})();
