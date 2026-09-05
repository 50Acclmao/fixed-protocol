(async () => {
    console.log("[build] server updated direct-default proxy-party v2026-07-14");
    const { Worker } = await import("worker_threads");
    const path = await import("path");
    const { WebSocketServer, WebSocket } = await import("ws");
    const { pack, unpack } = await import("msgpackr");
    const http = await import("http");
    const fs = await import("fs");
    const childProcess = await import("child_process");
    const fetchModule = await import("node-fetch");
    const realFetch = fetchModule.default || fetchModule;

    const __log = console.log;
    const __error = console.error;

    console.log = (...args) => {
        __log(`[${new Date().toISOString()}]`, ...args);
    };

    console.error = (...args) => {
        __error(`[${new Date().toISOString()}]`, ...args);
    };


    const DEFAULT_DECODO_PROXY = "http://spjkufyo3c:bc9QQa_elQYmp63qg5@dc.decodo.com:10000/";
    const PROXIES = (process.env.ARRAS_PROXY_URLS || process.env.ARRAS_PROXY_URL || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
    const prod = false;
    const envInt = (name, fallback, min = 0) => {
        const value = Number.parseInt(process.env[name] || "", 10);
        return Number.isFinite(value) && value >= min ? value : fallback;
    };
    const WORKER_MEMORY_MB = envInt("ARRAS_WORKER_MEMORY_MB", 384, 64);
    const BOTS_PER_WORKER = envInt("ARRAS_BOTS_PER_WORKER", 2, 1);
    const PREWARM_POOL_SIZE = envInt("ARRAS_PREWARM_POOL_SIZE", 0, 0);
    const EXIT_ON_WASABI_KEY_TRACE = process.env.ARRAS_EXIT_ON_WASABI_KEY_TRACE === "1";
    const SPAWN_BASE_DELAY_MS = 300;
    const SPAWN_JITTER_MS = 120;
    const DIRECT_STATUS_URLS = (process.env.ARRAS_STATUS_URLS || [
        "https://ak7oqfc2u4qqcu6i-c.uvwx.xyz:8443/2222/status",
        "https://qrp6ujau11f36bnm-c.uvwx.xyz:8443/2222/status",
        "https://kvn3s3cpcdk4fl6j-c.uvwx.xyz:8443/2222/status"
    ].join(",")).split(",").map((value) => value.trim()).filter(Boolean);
    const DIRECT_SOCKET_RESOLVE = process.env.ARRAS_DIRECT_SOCKET_RESOLVE !== "0";
    const DIRECT_SOCKET_PROBE = process.env.ARRAS_DIRECT_SOCKET_PROBE === "1";
    const ARRAS_WS_PROTOCOLS = ["arras.io#v1.4+sls+et0", "arras.io"];
    let arrasScriptCache = null;
    let arrasWasmCache = null;
    // HTTP SERVER
    const server = http.createServer((req, res) => {
        res.writeHead(426, { "Content-Type": "text/plain" });
        res.end("lll elk ez big fat noob");
    });


    // WS SERVER
    function randint(a, b) {
        return Math.floor(Math.random() * (b - a + 1)) + a;
    }

    const botWorkerPath = path.join(__dirname, "index.js");

    function extractArrasScript(html) {
        const scriptTagStart = html.indexOf("<script>");
        if (scriptTagStart === -1) {
            throw new Error("Could not find arras script tag");
        }
        const scriptStart = scriptTagStart + 8;
        const scriptTagEnd = html.indexOf("</script", scriptStart);
        if (scriptTagEnd === -1) {
            throw new Error("Could not find arras script close tag");
        }
        return html.slice(scriptStart, scriptTagEnd);
    }

    async function preloadArrasAssets() {
        try {
            console.log("Preloading arras script + wasm...");
            const preferredInstrumentedWasmPath = path.join(__dirname, "out", "arras-app-live-plainhook.wasm");
            delete process.env.ARRAS_WASABI_JS_PATH;
            const html = await (await realFetch("https://arras.io")).text();

            // Prefer the LIVE app.wasm to match the current inline script —
            // stale instrumented builds pair badly with a freshly-updated
            // game script and break the vm workers (no socket, no spawn).
            let wasmBytes = null;
            try {
                const wasmResponse = await realFetch("https://arras.io/app.wasm", { timeout: 15000 });
                const buf = await wasmResponse.arrayBuffer();
                if (buf && buf.byteLength) {
                    wasmBytes = Buffer.from(buf);
                    console.log("Live app.wasm fetched:", wasmBytes.byteLength, "bytes");
                }
            } catch (err) {
                console.warn("Live app.wasm fetch failed:", err && err.message ? err.message : err);
            }

            if (!wasmBytes && fs.existsSync(preferredInstrumentedWasmPath)) {
                wasmBytes = fs.readFileSync(preferredInstrumentedWasmPath);
                console.log("Falling back to instrumented (local) wasm:", wasmBytes.byteLength, "bytes");
            }
            if (!wasmBytes) {
                throw new Error("No usable Arras wasm (live fetch failed and no local wasm)");
            }

            arrasScriptCache = extractArrasScript(html);
            arrasWasmCache = new Uint8Array(wasmBytes);
            console.log(
                "Arras preload ready:",
                arrasScriptCache.length,
                "script chars,",
                arrasWasmCache.byteLength,
                "wasm bytes"
            );
        } catch (err) {
            console.error("Arras preload failed:", err);
        }
    }

    function createBotWorker(session, options = {}) {
        const worker = new Worker(botWorkerPath, {
            env: options.env || process.env,
            resourceLimits: {
                maxOldGenerationSizeMb: WORKER_MEMORY_MB,
                maxYoungGenerationSizeMb: 16,
                codeRangeSizeMb: 16,
            }
        });
        worker.send = (message) => worker.postMessage(message);
        worker.botId = null;
        worker.botIds = [];
        worker.activeBots = 0;
        worker.isPooled = false;
        worker.on("error", (err) => {
            console.error(`Bot worker ${worker.botId ?? "?"} error:`, err);
        });
        worker.on("message", (message) => {
            if (message?.type === "wasabi-key-trace-complete") {
                const suffix = EXIT_ON_WASABI_KEY_TRACE ? "exiting" : "continuing";
                console.log(
                    `[wasabi-key-trace] capture complete; ` +
                    `producerWindows=${message.producerWindows}; ${suffix}`
                );
                if (EXIT_ON_WASABI_KEY_TRACE) {
                    setTimeout(() => process.exit(0), 100);
                }
                return;
            }
            if (message?.type === "socket_url" && message.socketUrl) {
                const latestSocketUrlPath = path.join(__dirname, "latest-socket-url.txt");
                const socketUrl = String(message.socketUrl).trim();
                try {
                    fs.writeFileSync(latestSocketUrlPath, socketUrl);
                } catch (err) {
                    console.error("[socket-resolve] failed to write latest socket URL:", err.message);
                }
                console.log(`[socket-resolve] fresh vm-captured url for ${worker.resolveRequest?.hash || "unknown"}: ${socketUrl}`);
                if (socketUrl && worker.resolveRequest?.launchProtocol) {
                    launchProtocolOnlyClients(
                        session,
                        worker.resolveRequest.ws,
                        worker.resolveRequest.hash,
                        socketUrl,
                        worker.resolveRequest
                    );
                }
                if (worker.resolveRequest?.ws?.readyState === 1) {
                    worker.resolveRequest.ws.send(pack([
                        worker.resolveRequest.launchProtocol ? "P" : "U",
                        worker.resolveRequest.hash,
                        socketUrl,
                        socketUrl ? null : "missing-url"
                    ]));
                }
                return;
            }
            if (message?.type === "out_packets" && Array.isArray(message.packets)) {
                try {
                    fs.writeFileSync(
                        path.join(__dirname, "latest-captured-packets.json"),
                        JSON.stringify({ hash: message.hash || "", packets: message.packets }, null, 2)
                    );
                    console.log(`[packet-capture] saved ${message.packets.length} outbound packet(s) from vm for ${message.hash || "unknown"}`);
                } catch (err) {
                    console.error("[packet-capture] failed to write packets:", err.message);
                }
                return;
            }
            if (message?.type === "log" && typeof message.message === "string" && message.message.includes("[socket-capture] wrote ")) {
                const latestSocketUrlPath = path.join(__dirname, "latest-socket-url.txt");
                let socketUrl = "";
                try {
                    socketUrl = fs.readFileSync(latestSocketUrlPath, "utf8").trim();
                } catch (err) {
                    console.error("[socket-resolve] failed to read latest socket URL:", err.message);
                }
                console.log(`[socket-resolve] url captured for ${worker.resolveRequest?.hash || "unknown"}: ${socketUrl || "(missing)"}`);
                if (socketUrl && worker.resolveRequest?.launchProtocol) {
                    launchProtocolOnlyClients(
                        session,
                        worker.resolveRequest.ws,
                        worker.resolveRequest.hash,
                        socketUrl,
                        worker.resolveRequest
                    );
                }
                if (worker.resolveRequest?.ws?.readyState === 1) {
                    worker.resolveRequest.ws.send(pack([
                        worker.resolveRequest.launchProtocol ? "P" : "U",
                        worker.resolveRequest.hash,
                        socketUrl,
                        socketUrl ? null : "missing-url"
                    ]));
                }
                return;
            }
            if (!message || message.type !== "log") { return; }
            console.log(`[bot ${message.id ?? worker.botId ?? "?"}] ${message.message}`);
        });
        worker.on("exit", (code) => {
            let idx = session.workers.indexOf(worker);
            if (idx !== -1) {
                session.workers.splice(idx, 1);
            }
            idx = session.pool.indexOf(worker);
            if (idx !== -1) {
                session.pool.splice(idx, 1);
            }
            if (code !== 0) {
                console.log(`Bot worker ${worker.botId ?? "?"} exited with code`, code);
            }
        });
        return worker;
    }

    function prepareWorker(worker) {
        worker.send({
            type: "prepare",
            arrasCache: arrasScriptCache,
            wasmCache: arrasWasmCache,
        });
    }

    function fillPool(session) {
        while (session.pool.length < PREWARM_POOL_SIZE) {
            const worker = createBotWorker(session);
            worker.isPooled = true;
            session.pool.push(worker);
            prepareWorker(worker);
        }
    }

    function acquireWorker(session) {
        let worker = session.workers.find((candidate) => candidate.activeBots < BOTS_PER_WORKER);
        if (worker) {
            return worker;
        }

        worker = session.pool.shift() || createBotWorker(session);
        worker.isPooled = false;
        if (!session.workers.includes(worker)) {
            session.workers.push(worker);
        }
        return worker;
    }

    function queueBotSpawn(session, hash, botName) {
        session.spawnQueue.push({ hash, botName });
        processSpawnQueue(session);
    }

    function launchProtocolOnlyClients(session, ws, hash, socketUrl, options = {}) {
        const count = Math.max(1, Math.min(parseInt(options.count, 10) || 1, 50));
        const requestedDelay = parseInt(options.delay, 10);
        const delay = Math.max(0, Number.isFinite(requestedDelay) ? requestedDelay : (count > 1 ? 500 : 0));
        const botName = String(options.botName || "thara").trim() || "thara";
        const party = String(hash || "").replace(/^#/, "").match(/\d+$/)?.[0] || "";
        const scriptPath = path.join(__dirname, "protocol-only-random-client.js");
        const protocolProxyUrl = PROXIES.length ? PROXIES[session.proxyIdx % PROXIES.length] : "";
        console.log(`[protocol-only] launching count=${count} delay=${delay}ms hash=${hash} proxy=decodo`);
        const shouldPrintProtocolLine = (line) =>
            /\b(WebSocket open|Handshake complete|post-spawn accept|You have spawned|WebSocket error|WebSocket closed|\[retry\]|temporarily banned|blacklisted|Took too long|exited pid|death detected|respawn scheduled|reconnecting after death)\b/i.test(line) ||
            /^\[build\]/.test(line) ||
            /^\[INFO\] mode=/.test(line) ||
            /^\[INFO\] bot-position/.test(line) ||
            /^\[OUT\] spawn name=/.test(line) ||
            /^\[OUT\] spawn minimal/.test(line) ||
            /^\[OUT\] spawn party-shape/.test(line) ||
            /^\[OUT\] hello/.test(line) ||
            /^\[OUT\] fingerprint/.test(line) ||
            /^\[stderr\]/.test(line);

        const runtimeLogFiles = [
            "protocol-only-child.log",
            "protocol-only-validation.ndjson",
            "validation-inbound.ndjson",
            "protocol-only-u-samples.ndjson",
            "protocol-only-last-e.txt",
            "protocol-only-last-e.json",
            "protocol-only-last-k.txt"
        ];

        for (const file of runtimeLogFiles) {
            fs.writeFileSync(path.join(__dirname, file), "");
        }

        const childLogPath = path.join(__dirname, "protocol-only-child.log");
        fs.writeFileSync(childLogPath, `[start-batch] count=${count} hash=${hash} socket=${socketUrl}\n`);

        for (let i = 0; i < count; i++) {
            const timer = setTimeout(() => {
                const clientLogId = `${hash || "bot"}-${i + 1}`;
                const child = childProcess.spawn(process.execPath, [scriptPath], {
                    cwd: __dirname,
                    env: {
                        ...process.env,
                        ARRAS_SOCKET_URL: socketUrl,
                        ARRAS_CAPTURE_HASH: `#${hash}`,
                        ARRAS_BOT_NAME: botName,
                        ARRAS_PARTY: party,
                        ARRAS_LOG_U: "0",
                        ARRAS_CLIENT_LOG_ID: clientLogId,
                        ARRAS_PROXY_URL: protocolProxyUrl
                    },
                    stdio: ["ignore", "pipe", "pipe", "ipc"]
                });

                session.protocolClients.add(child);
                console.log(`[protocol-only] started pid=${child.pid} id=${clientLogId} hash=${hash} name=${JSON.stringify(botName)}`);
                sendProtocolChild(session, child, { type: "tankselect", tank: session.tank });
                fs.appendFileSync(childLogPath, `[start] pid=${child.pid} id=${clientLogId} hash=${hash} socket=${socketUrl}\n`);
                child.stdout.on("data", (chunk) => {
                    String(chunk).split(/\r?\n/).filter(Boolean).forEach((line) => {
                        fs.appendFileSync(childLogPath, `[stdout] ${line}\n`);
                        if (shouldPrintProtocolLine(line)) {
                            console.log(`[protocol-only ${child.pid}] ${line}`);
                        }
                    });
                });
                child.stderr.on("data", (chunk) => {
                    String(chunk).split(/\r?\n/).filter(Boolean).forEach((line) => {
                        fs.appendFileSync(childLogPath, `[stderr] ${line}\n`);
                        console.error(`[protocol-only ${child.pid}] ${line}`);
                    });
                });
                child.on("error", (error) => {
                    fs.appendFileSync(childLogPath, `[error] ${error && error.message ? error.message : error}\n`);
                    session.protocolClients.delete(child);
                });
                child.on("exit", (code, signal) => {
                    session.protocolClients.delete(child);
                    fs.appendFileSync(childLogPath, `[exit] code=${code} signal=${signal || ""}\n`);
                    console.log(`[protocol-only] exited pid=${child.pid} code=${code} signal=${signal || ""}`);
                });
            }, i * delay);
            session.spawnTimers.add(timer);
        }
        session.proxyIdx++;

    }

    function stopProtocolOnlyClients(session) {
        for (const child of session.protocolClients) {
            try {
                child.kill();
            } catch { }
        }
        session.protocolClients.clear();
    }

    function sendProtocolChild(session, child, message) {
        if (!child || !child.connected || child.killed || child.exitCode !== null || child.signalCode !== null) {
            session.protocolClients.delete(child);
            return;
        }
        try {
            child.send(message, (error) => {
                if (error) {
                    session.protocolClients.delete(child);
                }
            });
        } catch {
            session.protocolClients.delete(child);
        }
    }

    function readTailText(filePath, maxBytes = 1048576) {
        try {
            const stat = fs.statSync(filePath);
            const start = Math.max(0, stat.size - maxBytes);
            const fd = fs.openSync(filePath, "r");
            const buffer = Buffer.alloc(stat.size - start);
            fs.readSync(fd, buffer, 0, buffer.length, start);
            fs.closeSync(fd);
            return buffer.toString("utf8");
        } catch {
            return "";
        }
    }

    function getKnownArrasBuildId() {
        if (/^[a-f0-9]{16}$/i.test(process.env.ARRAS_BUILD_ID || "")) {
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
            const text = readTailText(path.join(__dirname, file));
            const matches = [
                ...[...text.matchAll(/[?&]b=([a-f0-9]{16})/gi)].map((match) => match[1]),
                ...[...text.matchAll(/"b"\s*:\s*"([a-f0-9]{16})"/gi)].map((match) => match[1])
            ];
            if (matches.length) {
                return matches[matches.length - 1];
            }
        }
        return "";
    }

    function getBrowserProvenSocketTimestamp(buildId) {
        if (/^\d{8,12}$/.test(process.env.ARRAS_SOCKET_T || "")) {
            return process.env.ARRAS_SOCKET_T;
        }
        const escapedBuild = String(buildId || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        if (!escapedBuild) {
            return "";
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
        const urlPattern = new RegExp(`[?&]b=${escapedBuild}(?:&[^\\s"'<>]*)?&t=(\\d{8,12})`, "gi");
        const jsonPattern = new RegExp(`"b"\\s*:\\s*"${escapedBuild}"[\\s\\S]{0,300}?"t"\\s*:\\s*"(\\d{8,12})"`, "gi");
        for (const file of files) {
            const text = readTailText(path.join(__dirname, file), 4 * 1048576);
            const matches = [
                ...[...text.matchAll(urlPattern)].map((match) => match[1]),
                ...[...text.matchAll(jsonPattern)].map((match) => match[1])
            ];
            if (matches.length) {
                return matches[matches.length - 1];
            }
        }
        return "";
    }

    async function fetchJsonWithTimeout(fetchUrl, timeoutMs = 3000) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        try {
            const response = await realFetch(fetchUrl, { signal: controller.signal });
            return await response.json();
        } finally {
            clearTimeout(timer);
        }
    }

    async function probeSocketUrl(socketUrl, timeoutMs = 2500) {
        return await new Promise((resolve, reject) => {
            const socket = new WebSocket(socketUrl, ARRAS_WS_PROTOCOLS, {
                headers: {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                    "accept-encoding": "gzip, deflate, br, zstd",
                    "accept-language": "en-US,en;q=0.9",
                    "origin": "https://arras.io",
                    "cache-control": "no-cache",
                    "pragma": "no-cache"
                },
                origin: "https://arras.io"
            });
            let settled = false;
            const done = (err) => {
                if (settled) { return; }
                settled = true;
                clearTimeout(timer);
                try { socket.close(); } catch { }
                if (err) { reject(err); } else { resolve(); }
            };
            const timer = setTimeout(() => done(new Error("probe-timeout")), timeoutMs);
            socket.once("open", () => done());
            socket.once("error", (err) => done(err || new Error("probe-error")));
            socket.once("close", () => done(new Error("probe-closed-before-open")));
        });
    }

    async function resolveSocketUrlDirect(hash) {
        const normalizedHash = String(hash || "").replace(/^#/, "").trim();
        const statusKeys = [normalizedHash];
        const withoutPartyDigits = normalizedHash.replace(/\d+$/, "");
        if (withoutPartyDigits && withoutPartyDigits !== normalizedHash) {
            statusKeys.push(withoutPartyDigits);
        }
        const buildId = getKnownArrasBuildId();
        if (!buildId) {
            throw new Error("missing-build-id");
        }
        let lastError = null;
        for (const statusUrl of DIRECT_STATUS_URLS) {
            try {
                const statusJson = await fetchJsonWithTimeout(statusUrl);
                let row = null;
                let statusKey = "";
                for (const candidate of statusKeys) {
                    const candidateRow = statusJson?.status?.[candidate];
                    if (candidateRow?.online && candidateRow.host) {
                        row = candidateRow;
                        statusKey = candidate;
                        break;
                    }
                }
                if (!row?.online || !row.host) {
                    continue;
                }
                const timestamp = getBrowserProvenSocketTimestamp(buildId);
                if (!timestamp) {
                    throw new Error("missing-browser-proven-t");
                }
                const socketUrl = `wss://${row.host}/?a=3&b=${buildId}&t=${timestamp}`;
                if (DIRECT_SOCKET_PROBE) {
                    await probeSocketUrl(socketUrl);
                }
                return { socketUrl, buildId, statusUrl, timestamp, statusKey };
            } catch (err) {
                lastError = err;
            }
        }
        throw lastError || new Error("missing-status-row");
    }

    async function resolveSocketUrlOnly(session, ws, hash, options = {}) {
        const normalizedHash = String(hash || "").replace(/^#/, "").trim();
        if (!normalizedHash) {
            if (ws.readyState === 1) {
                ws.send(pack([options.launchProtocol ? "P" : "U", "", "", "missing-hash"]));
            }
            return;
        }
        // Protocol deploys must use a FRESH socket URL captured from the live
        // vm client — the direct path replays stale build/t from old traces and
        // the game server kills those connections immediately (1006).
        if (DIRECT_SOCKET_RESOLVE && !options.launchProtocol) {
            try {
                const direct = await resolveSocketUrlDirect(normalizedHash);
                const statusKeyText = direct.statusKey && direct.statusKey !== normalizedHash ? ` statusKey=${direct.statusKey}` : "";
                console.log(`[socket-resolve] direct url for ${normalizedHash}: ${direct.socketUrl} build=${direct.buildId} t=${direct.timestamp}${statusKeyText}`);
                if (options.launchProtocol) {
                    launchProtocolOnlyClients(session, ws, normalizedHash, direct.socketUrl, options);
                }
                if (ws.readyState === 1) {
                    ws.send(pack([options.launchProtocol ? "P" : "U", normalizedHash, direct.socketUrl, null]));
                }
                return;
            } catch (err) {
                console.log(`[socket-resolve] direct failed for ${normalizedHash}: ${err.message}; falling back to headless`);
            }
        }
        try {
            fs.rmSync(path.join(__dirname, "latest-socket-url.txt"), { force: true });
        } catch { }

        const worker = createBotWorker(session, {
            env: {
                ...process.env,
                ARRAS_CAPTURE_SOCKET_URL_ONLY: "1",
                ARRAS_PREWARM_POOL_SIZE: "0",
                ARRAS_BOTS_PER_WORKER: "1",
                ARRAS_WORKER_MEMORY_MB: String(WORKER_MEMORY_MB)
            }
        });
        worker.resolveRequest = {
            ws,
            hash: normalizedHash,
            launchProtocol: Boolean(options.launchProtocol),
            count: options.count,
            botName: options.botName,
            delay: options.delay
        };
        session.workers.push(worker);

        console.log(`[socket-resolve] resolving hash: ${normalizedHash}`);
        worker.send({
            type: "start", config: {
                id: `resolve-${Date.now()}`,
                ...(PROXIES.length ? { proxy: { type: "http", url: PROXIES[session.proxyIdx % PROXIES.length] } } : {}),
                hash: "#" + normalizedHash,
                name: "resolver",
                stats: [0, 0, 0, 0, 0, 0, 0, 9],
                type: "manual",
                token: "resolve-url",
                autoFire: false,
                autoRespawn: false,
                keys: [],
                keysHold: [],
                tank: "Basic",
                chatSpam: "",
                initialTarget: { tank: session.tank || "basic" },
                squadId: normalizedHash,
                reconnectAttempts: 0,
                reconnectDelay: 8000,
                arrasCache: arrasScriptCache,
                wasmCache: arrasWasmCache,
            }
        });

        session.proxyIdx++;
    }

    function processSpawnQueue(session) {
        if (session.spawnQueueActive) { return; }
        const job = session.spawnQueue.shift();
        if (!job) {
            fillPool(session);
            return;
        }

        session.spawnQueueActive = true;
        const botId = session.nextBotId++;
        const spawnDelay = SPAWN_BASE_DELAY_MS + randint(0, SPAWN_JITTER_MS);

        session.spawnTimer = setTimeout(() => {
            session.spawnTimer = null;
            if (session.proxyIdx >= PROXIES.length) {
                session.proxyIdx = 0;
            }

            const worker = acquireWorker(session);
            worker.botId = botId;
            worker.botIds.push(botId);
            worker.activeBots++;
            console.log(`Starting bot ${botId} in worker slot ${worker.activeBots}/${BOTS_PER_WORKER} after queued ${spawnDelay}ms (${session.spawnQueue.length} waiting)`);

            let selectedTank = session.tank;
            if (session.tanks.length) {
                selectedTank = session.tanks[session.tankIdx];
                session.tankIdx++;
                if (session.tankIdx >= session.tanks.length) {
                    session.tankIdx = 0;
                }
            }

            worker.send({
                type: "start", config: {
                    id: botId,
                    ...(PROXIES.length ? { proxy: { type: "http", url: PROXIES[session.proxyIdx % PROXIES.length] } } : {}),
                    hash: "#" + job.hash,
                    name: job.botName,
                    stats: [0, 0, 0, 0, 0, 0, 0, 9],
                    type: "manual",
                    token: "manual-control",
                    autoFire: false,
                    autoRespawn: true,
                    keys: [],
                    keysHold: [],
                    tank: "Auto4",
                    chatSpam: "",
                    initialTarget: { tank: selectedTank },
                    squadId: job.hash,
                    reconnectAttempts: 5,
                    reconnectDelay: 8000,
                    arrasCache: arrasScriptCache,
                    wasmCache: arrasWasmCache,
                }
            });

            session.proxyIdx++;
            session.spawnQueueActive = false;
            processSpawnQueue(session);
        }, spawnDelay);
    }

    const sessions = new Map();
    const wss = new WebSocketServer({ server });

    wss.on("connection", (ws, req) => {
        const addr = req.socket.remoteAddress;
        console.log(addr, "connected");

        // Initialize or retrieve session for this IP
        if (!sessions.has(addr)) {
            sessions.set(addr, {
                workers: [],
                pool: [],
                spawnQueue: [],
                spawnQueueActive: false,
                spawnTimer: null,
                spawnTimers: new Set(),
                protocolClients: new Set(),
                nextBotId: 0,
                tank: "auto6",
                tanks: [],
                tankIdx: 0,
                proxyIdx: 0
            });
        }
        const session = sessions.get(addr);

        let challenge;
        let verified = false;

        function packet(...args) {
            ws.send(pack(args));
        }

        function close() {
            ws.close();
            // We only destroy workers if explicitly told to, or if the session is terminated.
            // For now, we don't destroy them on socket close to support refresh.
        }

        ws.on("message", (msg) => {
            try {
                const data = unpack(msg);
                const type = data.shift();

                switch (type) {
                    case "M":
                        if (challenge || data[0] != 72011) {
                            close();
                        }

                        challenge = randint(0b1000000000, 0b1111111111);
                        packet("M", challenge);
                        break;

                    case "C":
                        if (data[0] == (challenge ^ 845)) {
                            verified = true;
                            console.log(addr, "verified");
                            fillPool(session);
                        } else {
                            close();
                            console.log(addr, "true noob")
                        }

                        break;

                    case "Z":
                        session.tank = data[0];
                        if (session.tank instanceof Array) {
                            session.tanks = session.tank;
                            session.tankIdx = 0;

                            for (const worker of session.workers) {
                                for (const botId of worker.botIds) {
                                    const t = session.tanks[session.tankIdx];
                                    worker.send({ type: "tankselect", tank: t, botId });

                                    session.tankIdx++;
                                    if (session.tankIdx >= session.tanks.length) {
                                        session.tankIdx = 0;
                                    }
                                }
                            }
                            let protocolTankIdx = 0;
                            for (const child of session.protocolClients) {
                                const t = session.tanks[protocolTankIdx];
                                sendProtocolChild(session, child, { type: "tankselect", tank: t });
                                protocolTankIdx++;
                                if (protocolTankIdx >= session.tanks.length) {
                                    protocolTankIdx = 0;
                                }
                            }
                        } else {
                            session.tanks = [];
                            for (const worker of session.workers) {
                                worker.send({ type: "tankselect", tank: session.tank })
                            }
                            for (const child of session.protocolClients) {
                                sendProtocolChild(session, child, { type: "tankselect", tank: session.tank });
                            }
                        }

                        break;

                    case "F":
                        if (verified) {
                            const hash = data[0];
                            const count = parseInt(data[1]) || 1;
                            const botName = String(data[2] || "thara's Bot").trim() || "thara's Bot";

                            console.log(`Queueing ${count} bots for hash: ${hash}`);
                            for (let i = 0; i < count; i++) {
                                queueBotSpawn(session, hash, botName);
                            }
                        }

                        break;

                    case "U":
                        if (verified) {
                            const hash = data[0];
                            resolveSocketUrlOnly(session, ws, hash);
                        }

                        break;

                    case "P":
                        if (verified) {
                            const hash = data[0];
                            const count = Math.max(1, parseInt(data[1], 10) || 1);
                            const botName = String(data[2] || "thara").trim() || "thara";
                            const requestedDelay = parseInt(data[3], 10);
                            const options = {
                                launchProtocol: true,
                                count,
                                botName
                            };
                            if (Number.isFinite(requestedDelay) && requestedDelay > 0) {
                                options.delay = requestedDelay;
                            }
                            resolveSocketUrlOnly(session, ws, hash, {
                                ...options
                            });
                        }

                        break;

                    case "B":
                        if (verified) {
                            session.spawnQueue = [];
                            session.spawnQueueActive = false;
                            if (session.spawnTimer) {
                                clearTimeout(session.spawnTimer);
                                session.spawnTimer = null;
                            }
                            for (const timer of session.spawnTimers) {
                                clearTimeout(timer);
                            }
                            session.spawnTimers.clear();
                            for (const worker of session.workers) {
                                worker.send({ type: "destroy" });
                            }
                            session.workers = [];
                            stopProtocolOnlyClients(session);
                        }

                        break;

                    case "A":
                        if (verified) {
                            for (const worker of session.workers) {
                                worker.send({
                                    type: "position",
                                    x: data[0],
                                    y: data[1],
                                    mouseX: data[2],
                                    mouseY: data[3],
                                    mouseDown: data[4],
                                    rMouseDown: data[5],
                                    mouse: data[6],
                                    feeding: data[7],
                                    shift: data[8],
                                    autofire: data[9],
                                    autospin: data[10],
                                    manualMode: data[11],
                                    manualX: data[12],
                                    manualY: data[13],
                                    manualScaleX: data[15],
                                    manualScaleY: data[16]
                                });
                            }
                            for (const child of session.protocolClients) {
                                sendProtocolChild(session, child, {
                                    type: "position",
                                    x: data[0],
                                    y: data[1],
                                    mouseX: data[2],
                                    mouseY: data[3],
                                    mouseDown: data[4],
                                    rMouseDown: data[5],
                                    mouse: data[6],
                                    feeding: data[7],
                                    shift: data[8],
                                    autofire: data[9],
                                    autospin: data[10],
                                    manualMode: data[11],
                                    manualX: data[12],
                                    manualY: data[13],
                                    manualScaleX: data[15],
                                    manualScaleY: data[16]
                                });
                            }
                        }
                        break;

                    case "T":
                        if (verified) {
                            for (const worker of session.workers) {
                                worker.send({
                                    type: "chat",
                                    message: data[0],
                                    spam: data[1]
                                });
                            }
                        }
                        break;

                    default:
                        close();
                        break;
                }
            } catch (e) {
                console.error(e);
            }
        });

        ws.on("close", () => {
            console.log(addr, "disconnected (session retained)");
        });
    });


    const port = prod ? process.env.PORT : 8082;
    await preloadArrasAssets();
    server.listen(port, () => {
        console.log("Server listening on port!!!!", port);
    });
})();
