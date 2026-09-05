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

    const WORKER_MEMORY_MB = parseInt(process.env.ARRAS_WORKER_MEMORY_MB || "96", 10) || 96;
    const BOTS_PER_WORKER = parseInt(process.env.ARRAS_BOTS_PER_WORKER || "25", 10) || 25;
    const PREWARM_POOL_SIZE = parseInt(process.env.ARRAS_PREWARM_POOL || "16", 10) || 16;
    const MAX_PROXIES = parseInt(process.env.ARRAS_MAX_PROXIES || "6000", 10) || 6000;
    const MAX_WORKERS = parseInt(process.env.ARRAS_MAX_WORKERS || "128", 10) || 128;
    const MAX_BOTS_GLOBAL = Math.max(50, parseInt(process.env.ARRAS_MAX_BOTS || "1500", 10) || 1500);
    const PROXY_REFRESH_MS = parseInt(process.env.ARRAS_PROXY_REFRESH_MS || "180000", 10) || 180000;
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

    async function rankProxies() {
        if (!PROXY_POOL.length || isRankingProxies) return;
        isRankingProxies = true;

        try {
            // Cap the test load — checking 800 is plenty to find fast ones
            const candidates = PROXY_POOL.slice(0, 800);

            const results = await Promise.all(
                candidates.map(async (proxyUrl) => {
                    const start = Date.now();

                    try {
                        const res = await realFetch("https://arras.io", {
                            agent: new HttpsProxyAgent(proxyUrl),
                            timeout: 4000
                        });

                        await res.arrayBuffer();

                        return { proxyUrl, ms: Date.now() - start };
                    } catch {
                        return null;
                    }
                })
            );

            const ranked = results.filter(Boolean).sort((a, b) => a.ms - b.ms);

            // Rebuild the ranked queue every cycle. Defenders consume it with
            // shift(), so top it up from the untested tail to keep it from
            // starving on long 1500-bot runs.
            PROXY_RANKED = ranked
                .map((r) => r.proxyUrl)
                .concat(PROXY_POOL.filter((url) => !candidates.includes(url)));

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
            // farms (1000+ bots) keep spawning instead of dying out.
            if (PROXY_POOL.length) {
                session.proxyQueue = PROXY_POOL.slice();
                return session.proxyQueue.pop();
            }

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

        if (!proxyUrl) return false;

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

        const spawnHash = session.resolvedHash
            ? "#" + session.resolvedHash
            : "#" + rawHash;

        worker.send({
            type: "start",
            config: {
                id: botId,
                proxy: {
                    type: "http",
                    url: proxyUrl
                },
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
                initialTarget: {
                    tank: selectedTank,
                    isDefender: !!isDefender
                },
                squadId: rawHash,
                reconnectAttempts: 2,
                reconnectDelay: 12000,
                arrasCache: arrasScriptCache,
                wasmCache: arrasWasmCache,
                teamColor: session.teamColor
            }
        });

        totalSpawned++;
        activeBotCount++;

        return true;
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
            String(
                options.botName || "thara"
            ).trim() || "thara";

        const party =
            String(hash || "")
                .replace(/^#/, "")
                .match(/\d+$/)?.[0] || "";

        const scriptPath =
            path.join(
                __dirname,
                "protocol-only-random-client.js"
            );

        const proxyUrl =
            takeUniqueProxy(session);

        for (let i = 0; i < count; i++) {
            const timer = setTimeout(() => {
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

                    nextBotId: 0,

                    tank: "auto6",

                    tanks: [],

                    tankIdx: 0,

                    proxyQueue: [],

                    resolvedHash: null,

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

                                    let botName =
                                        "thara's Bot";

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
                                            String(
                                                b ??
                                                "thara's Bot"
                                            ).trim() ||
                                            "thara's Bot";
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
                                            String(
                                                a ??
                                                "thara's Bot"
                                            ).trim() ||
                                            "thara's Bot";

                                        count =
                                            Math.max(
                                                1,
                                                parseInt(
                                                    b,
                                                    10
                                                ) || 1
                                            );
                                    } else {
                                        botName =
                                            String(
                                                a ??
                                                b ??
                                                "thara's Bot"
                                            ).trim() ||
                                            "thara's Bot";

                                        count = 1;
                                    }

                                    count =
                                        Math.min(
                                            count,
                                            2000
                                        );

                                    for (
                                        let i = 0;
                                        i < count;
                                        i++
                                    ) {
                                        if (
                                            !spawnBotNow(
                                                session,
                                                hash,
                                                botName
                                            )
                                        ) {
                                            break;
                                        }
                                    }
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
                                            2000
                                        );

                                    const botName =
                                        String(
                                            data[2] ||
                                            "Defender"
                                        ).trim() ||
                                        "Defender";

                                    session.tanks = [
                                        "octo",
                                        "gale",
                                        "automingler"
                                    ];

                                    session.tankIdx = 0;

                                    for (
                                        let i = 0;
                                        i < count;
                                        i++
                                    ) {
                                        if (
                                            !spawnBotNow(
                                                session,
                                                hash,
                                                botName,
                                                true
                                            )
                                        ) {
                                            break;
                                        }
                                    }
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
                                        String(
                                            data[2] ||
                                            "thara"
                                        ).trim() ||
                                        "thara";

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

                                        manualMode: false,
                                        manualX: 0,
                                        manualY: 0,

                                        noMove: false,

                                        teamColor:
                                            session.teamColor
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
    rankProxies();
    preloadArrasAssets();

    // Keep the proxy pool fresh — 1500 bots burn through proxies on every
    // reconnect/respawn, so a one-shot fetch starves after a few minutes.
    setInterval(() => {
        fetchProxies().then(() => rankProxies());
    }, PROXY_REFRESH_MS);

    // Cheap liveness telemetry so long headless runs can be observed.
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
