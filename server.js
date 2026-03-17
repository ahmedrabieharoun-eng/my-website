// ================================================================
//  PANDA BAMBOO FACTORY — Cloudflare Worker v3.0
//  Firebase Realtime Database
//  Environment Variables:
//    FIREBASE_DATABASE_URL  e.g. https://YOUR-DB.firebaseio.com
//    FIREBASE_API_KEY       Firebase API key
//    BOT_TOKEN              Telegram Bot Token
//    ADMIN_IDS              comma-separated admin Telegram IDs
// ================================================================

const G = {
  BAMBOO_PER_COIN:10, TON_PER_COIN:0.00005, TON_TO_BAMBOO:10000,
  MIN_WITHDRAW:200, MIN_DEPOSIT_TON:1,
  REF_BONUS_PCT:20,
  WELCOME_BAMBOO:0,
  WELCOME_COINS :200,
  WELCOME_RATE  :4.167, // Fix: 100 bamboo/day (100/24) for new users
  MAX_TANK_LVL:27,      // Fix: 27 real levels
  MAX_RETRY:3, RETRY_DELAY_MS:100,
  // Fix 4: rebalanced items — ROI increases with tier, no overlap
  ITEMS:{
    bamboo_stick :{price:1500,    power:50    },  // 0.033 bam/hr per coin — entry
    panda_paw    :{price:5000,    power:200   },  // 0.040 — slightly better
    leaf_fan     :{price:25000,   power:1200  },  // 0.048 — noticeably better
    bamboo_energy:{price:125000,  power:7500  },  // 0.060 — clearly better
    panda_den    :{price:626000,  power:45000 },  // 0.072 — premium
    bamboo_forest:{price:1300000, power:110000},  // 0.085 — endgame
  },
  // 27 tank levels — capacity only, no speedBonus
  TANK:{
    1 :{cap:300,     upgCost:500      },
    2 :{cap:900,     upgCost:2000     },
    3 :{cap:2400,    upgCost:8000     },
    4 :{cap:6000,    upgCost:25000    },
    5 :{cap:18000,   upgCost:80000    },
    6 :{cap:60000,   upgCost:250000   },
    7 :{cap:120000,  upgCost:500000   },
    8 :{cap:210000,  upgCost:900000   },
    9 :{cap:330000,  upgCost:1500000  },
    10:{cap:480000,  upgCost:2500000  },
    11:{cap:660000,  upgCost:4000000  },
    12:{cap:870000,  upgCost:6000000  },
    13:{cap:1110000, upgCost:9000000  },
    14:{cap:1380000, upgCost:13000000 },
    15:{cap:1680000, upgCost:18000000 },
    16:{cap:2010000, upgCost:25000000 },
    17:{cap:2400000, upgCost:33000000 },
    18:{cap:2850000, upgCost:43000000 },
    19:{cap:3360000, upgCost:55000000 },
    20:{cap:3930000, upgCost:70000000 },
    21:{cap:4560000, upgCost:88000000 },
    22:{cap:5250000, upgCost:110000000},
    23:{cap:6000000, upgCost:135000000},
    24:{cap:6810000, upgCost:165000000},
    25:{cap:7680000, upgCost:200000000},
    26:{cap:8610000, upgCost:240000000},
    27:{cap:10000000,upgCost:300000000},
  },
  // Fix 5: Added r200 and r500
  REF_TASKS:{
    r1  :{n:1,   bam:500,    coins:2   },
    r5  :{n:5,   bam:2500,   coins:10  },
    r10 :{n:10,  bam:6000,   coins:25  },
    r20 :{n:20,  bam:15000,  coins:60  },
    r50 :{n:50,  bam:40000,  coins:150 },
    r70 :{n:70,  bam:60000,  coins:220 },
    r100:{n:100, bam:100000, coins:400 },
    r200:{n:200, bam:200000, coins:800 },
    r500:{n:500, bam:500000, coins:2000},
  },
  SOC_TASKS:{
    tg_payouts:1000,  // قناة المدفوعات — مطلوبة
    tg_news   :500,   // قناة الأخبار — مطلوبة
    tg_ch     :1000,
    tg_grp    :500,
    tg_bot    :300,
  },
  BOT_USERNAME:'PandaBamboBot', // Fix 6
};

const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET, POST, OPTIONS','Access-Control-Allow-Headers':'Content-Type, Authorization, X-Action','Access-Control-Max-Age':'86400'};
const JSON_CT={'Content-Type':'application/json',...CORS};
const jRes=(b,s=200)=>new Response(JSON.stringify(b),{status:s,headers:JSON_CT});
const ok=d=>jRes({success:true,data:d});
const fail=(m,s=400)=>jRes({success:false,error:m},s);

function sanitise(i){if(!i)return i;return i.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi,'').replace(/[<>]/g,m=>m==='<'?'&lt;':'&gt;');}

// ── Firebase helpers ──────────────────────────────────────────────
function fbUrl(env,path){
  const b=env.FIREBASE_DATABASE_URL?.replace(/\/$/,'');
  if(!b)throw new Error('FIREBASE_DATABASE_URL not set');
  const k=env.FIREBASE_API_KEY;
  if(!k)throw new Error('FIREBASE_API_KEY not set');
  return `${b}/${path.replace(/^\//,'')}.json?key=${k}`;
}
async function dbGet(env,path){
  try{const r=await fetch(fbUrl(env,path));if(!r.ok)throw new Error(`GET ${r.status}`);return{success:true,data:await r.json()};}
  catch(e){console.error('DB GET',path,e.message);return{success:false,error:e.message};}
}
async function dbSet(env,path,data){
  try{const r=await fetch(fbUrl(env,path),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(!r.ok)throw new Error(`SET ${r.status}`);return{success:true};}
  catch(e){console.error('DB SET',path,e.message);return{success:false,error:e.message};}
}
async function dbUpdate(env,path,updates){
  try{const r=await fetch(fbUrl(env,path),{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(updates)});if(!r.ok)throw new Error(`UPDATE ${r.status}`);return{success:true};}
  catch(e){console.error('DB UPDATE',path,e.message);return{success:false,error:e.message};}
}
async function dbPush(env,path,data){
  try{const r=await fetch(fbUrl(env,path),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(!r.ok)throw new Error(`PUSH ${r.status}`);const j=await r.json();return{success:true,data:{id:j.name}};}
  catch(e){console.error('DB PUSH',path,e.message);return{success:false,error:e.message};}
}
async function dbDelete(env,path){
  try{const r=await fetch(fbUrl(env,path),{method:'DELETE'});if(!r.ok)throw new Error(`DELETE ${r.status}`);return{success:true};}
  catch(e){console.error('DB DELETE',path,e.message);return{success:false,error:e.message};}
}

// ── Rate limiter ──────────────────────────────────────────────────
const _rl=new Map();
function rateOk(ip){const now=Date.now();const d=_rl.get(ip)||{c:0,r:now+60000};if(now>d.r){d.c=0;d.r=now+60000;}d.c++;_rl.set(ip,d);return d.c<=60;}

// ── Logging System ────────────────────────────────────────────────
// Saves balance-change events inside each user's own account:
//   users/{uid}/log/{auto-id}
// Only records events that change bamboo, coins, or tonBalance.
// Fire-and-forget — never blocks the request.

const BALANCE_CHANGE_EVENTS = new Set([
  'collect','buy_item','upgrade_tank','exchange',
  'withdraw_request','deposit_completed','claim_task',
  'verify_task','create_task','admin_set_balance',
  'admin_confirm_deposit','referral_commission',
]);

function log(env, uid, type, details={}, meta={}){
  if(!BALANCE_CHANGE_EVENTS.has(type)) return;
  const ts   = Date.now();
  const date = new Date(ts).toISOString();
  const entry = { ts, date, type, ...details };
  dbPush(env, `users/${uid}/log`, entry)
    .catch(e=>console.error('LOG ERROR:',e.message));
}

// ── Telegram validation ───────────────────────────────────────────
async function validateTg(initData,botToken){
  try{
    if(!initData)return{valid:false,error:'No init data'};
    const p=new URLSearchParams(initData);
    // Extract start_param here — it lives as a top-level initData param
    const startParam=(p.get('start_param')||'').replace(/\D/g,'');
    if(!botToken){
      const u=p.get('user');
      if(!u)return{valid:false,error:'No user'};
      return{valid:true,user:JSON.parse(decodeURIComponent(u)),startParam};
    }
    const hash=p.get('hash');
    if(!hash)return{valid:false,error:'No hash'};
    p.delete('hash');
    const authDate=parseInt(p.get('auth_date')||'0');
    if(Date.now()/1000-authDate>900)return{valid:false,error:'Expired'};
    const dc=[...p.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>`${k}=${v}`).join('\n');
    const enc=new TextEncoder();
    const sec=await crypto.subtle.importKey('raw',enc.encode('WebAppData'),{name:'HMAC',hash:'SHA-256'},false,['sign']);
    const kb=await crypto.subtle.sign('HMAC',sec,enc.encode(botToken));
    const key=await crypto.subtle.importKey('raw',kb,{name:'HMAC',hash:'SHA-256'},false,['sign']);
    const sig=await crypto.subtle.sign('HMAC',key,enc.encode(dc));
    const hex=[...new Uint8Array(sig)].map(b=>b.toString(16).padStart(2,'0')).join('');
    if(hex!==hash)return{valid:false,error:'Bad hash'};
    const u=p.get('user');if(!u)return{valid:false,error:'No user'};
    return{valid:true,user:JSON.parse(decodeURIComponent(u)),startParam};
  }catch(e){return{valid:false,error:e.message};}
}

// ── Tank sync ─────────────────────────────────────────────────────
function syncTank(user){
  const now=Date.now();const sec=(now-(user.lastSeen||now))/1000;
  if(sec<=0||!user.miningRate){user.lastSeen=now;return;}
  const cfg=G.TANK[user.tankLevel||1]||G.TANK[1];
  const rate=user.miningRate/3600; // no speedBonus
  user.tankAccrued=Math.min(cfg.cap,(user.tankAccrued||0)+rate*sec);
  user.lastSeen=now;
}
function recalcRate(m){return Object.entries(m||{}).reduce((s,[id,c])=>s+(G.ITEMS[id]?.power||0)*c,0);}

async function registerReferral(env,uid,user,referrerId){
  try{
    const rr=await dbGet(env,`users/${referrerId}/referrals`);
    const refs=rr.data||{};
    if(!refs[uid]){
      await dbSet(env,`users/${referrerId}/referrals/${uid}`,{
        userId:uid,
        firstName:user.firstName,lastName:user.lastName,
        username:user.username,photoUrl:user.photoUrl,
        joinedAt:Date.now(),earned:0,
      });
      console.log(`Referral registered: ${uid} referred by ${referrerId}`);
    }
  }catch(e){console.error('registerReferral error:',e.message);}
}

function makeUser(uid,tg={},ref=null){
  return{userId:uid,firstName:(tg.first_name||'').slice(0,64),lastName:(tg.last_name||'').slice(0,64),username:(tg.username||'').slice(0,64),photoUrl:(tg.photo_url||'').slice(0,512),
    bamboo:G.WELCOME_BAMBOO, coins:G.WELCOME_COINS, miningRate:G.WELCOME_RATE,
    totalEarned:0,machines:{},tankLevel:1,tankAccrued:0,lastSeen:Date.now(),createdAt:Date.now(),
    welcomeBonusGiven:true,
    hasDeposited:false,tonBalance:0,referralCode:String(uid),referredBy:ref||null,completedTasks:[]};
}

// ── Extract start_param from Telegram initData string ────────────
function extractStartParam(initDataStr){
  try{
    const p=new URLSearchParams(initDataStr||'');
    // Direct start_param field
    const sp=p.get('start_param');
    if(sp) return sp.replace(/\D/g,'');
    // Sometimes inside user JSON
    const userRaw=p.get('user');
    if(userRaw){
      const u=JSON.parse(decodeURIComponent(userRaw));
      if(u.start_param) return String(u.start_param).replace(/\D/g,'');
    }
  }catch(_){}
  return '';
}

// ── Handlers ──────────────────────────────────────────────────────
async function hGetState(env,uid,tg,data={},_meta={}){
  try{
    // _startParam comes directly from initData (most reliable source)
    const rawRef = (
      data?._startParam ||
      extractStartParam(data?._initData||'') ||
      (data?.start_param||'').toString().replace(/\D/g,'')
    ).replace(/\D/g,'');
    const ref = rawRef && rawRef !== uid ? rawRef : null;

    const ur=await dbGet(env,`users/${uid}`);let user=ur.data;
    if(!user){
      user=makeUser(uid,tg,ref);
      if(user.referredBy){
        await registerReferral(env,uid,user,user.referredBy);
      }
      await dbSet(env,`users/${uid}`,user);
      log(env,uid,'register',{
        referredBy:ref||null,
        welcomeCoins:G.WELCOME_COINS,
        welcomeRate:G.WELCOME_RATE,
        welcomeBamboo:G.WELCOME_BAMBOO,
        username:user.username||'',
        firstName:user.firstName||'',
        platform:'telegram',
        ip_action:'new_user',
      },_meta);
    }else{
      syncTank(user);
      // Fix welcome bonus: grant once if flag not set yet
      let needsSave=false;
      if(!user.welcomeBonusGiven){
        user.coins      = (user.coins||0)      + G.WELCOME_COINS;
        user.bamboo     = (user.bamboo||0)      + G.WELCOME_BAMBOO;
        user.miningRate = Math.max(user.miningRate||0, G.WELCOME_RATE);
        user.welcomeBonusGiven = true;
        needsSave=true;
        console.log(`Welcome bonus granted to existing user ${uid}`);
        log(env,uid,'welcome_bonus_granted',{
          coins_added:G.WELCOME_COINS, bamboo_added:G.WELCOME_BAMBOO,
          miningRate_set:G.WELCOME_RATE,
        },_meta);
      }
      if(tg){
        if(tg.first_name) user.firstName=tg.first_name.slice(0,64);
        if(tg.last_name)  user.lastName =tg.last_name.slice(0,64);
        if(tg.username)   user.username =tg.username.slice(0,64);
        if(tg.photo_url)  user.photoUrl =tg.photo_url.slice(0,512);
      }
      await dbUpdate(env,`users/${uid}`,{
        firstName:user.firstName,lastName:user.lastName,
        username:user.username,photoUrl:user.photoUrl,
        tankAccrued:user.tankAccrued,lastSeen:user.lastSeen,
        ...(needsSave?{
          coins:user.coins,bamboo:user.bamboo,
          miningRate:user.miningRate,welcomeBonusGiven:true,
        }:{}),
      });
    }
    const rr=await dbGet(env,`users/${uid}/referrals`);
    const referrals=Object.values(rr.data||{}).map(r=>({userId:r.userId,name:`${r.firstName||''} ${r.lastName||''}`.trim()||'Friend',photo:r.photoUrl||null,date:r.joinedAt?new Date(r.joinedAt).toLocaleDateString():'',earned:r.earned||0}));
    // Log every session open (app open)
    log(env,uid,'session_open',{
      bamboo:user.bamboo||0, coins:user.coins||0,
      miningRate:user.miningRate||0, tankLevel:user.tankLevel||1,
      tankAccrued:user.tankAccrued||0,
    },_meta);
    const er=await dbGet(env,`users/${uid}/exchHistory`);
    const exchHistory=er.data?Object.values(er.data).sort((a,b)=>b.ts-a.ts).slice(0,30):[];
    const wr=await dbGet(env,`users/${uid}/wdHistory`);
    const wdHistory=wr.data?Object.values(wr.data).sort((a,b)=>b.ts-a.ts).slice(0,30):[];
    const dr=await dbGet(env,`users/${uid}/deposits`);
    const pendingDeposit=(dr.data?Object.values(dr.data):[]).find(d=>d.status==='pending')||null;
    // Load tasks from DB
    const tpr=await dbGet(env,'tasks/partner');
    const tcr=await dbGet(env,'tasks/community');
    const tasks={
      partner  :tpr.data?Object.values(tpr.data).filter(t=>t.status==='active'):[],
      community:tcr.data?Object.values(tcr.data).filter(t=>t.status==='active'):[],
    };
    return{success:true,data:{user:{bamboo:user.bamboo||0,coins:user.coins||0,miningRate:user.miningRate||0,totalEarned:user.totalEarned||0,machines:user.machines||{},tankLevel:user.tankLevel||1,tankAccrued:user.tankAccrued||0,hasDeposited:user.hasDeposited||false,tonBalance:user.tonBalance||0},referrals,completedTasks:user.completedTasks||[],exchHistory,wdHistory,pendingDeposit,tasks}};
  }catch(e){console.error('getState',e);return{success:false,error:e.message,errorCode:'GET_STATE_ERROR'};}
}

async function hCollect(env,uid,data,_meta={}){
  try{
    const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    syncTank(user);const actual=Math.floor(user.tankAccrued);
    if(actual<1)return{success:false,error:'Tank is empty'};
    const nb=(user.bamboo||0)+actual;
    await dbUpdate(env,`users/${uid}`,{bamboo:nb,totalEarned:(user.totalEarned||0)+actual,tankAccrued:user.tankAccrued-actual,lastSeen:user.lastSeen});
    log(env,uid,'collect',{
      collected:actual,
      bamboo_before:(user.bamboo||0),
      bamboo_after:nb,
      tankLevel:user.tankLevel||1,
    },_meta);
    return{success:true,data:{collected:actual,bamboo:nb}};
  }catch(e){return{success:false,error:e.message};}
}

async function hBuyItem(env,uid,data,_meta={}){
  try{
    const{itemId,qty=1}=data;const item=G.ITEMS[itemId];
    if(!item)return{success:false,error:'Unknown item'};
    const q=Math.max(1,Math.min(99,parseInt(qty)||1));const total=item.price*q;
    const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    if((user.bamboo||0)<total)return{success:false,error:'Not enough Bamboo'};
    const machines=user.machines||{};machines[itemId]=(machines[itemId]||0)+q;
    const newRate=recalcRate(machines);const nb=(user.bamboo||0)-total;
    await dbUpdate(env,`users/${uid}`,{bamboo:nb,machines,miningRate:newRate});
    log(env,uid,'buy_item',{
      itemId, qty:q, totalCost:total,
      bamboo_before:(user.bamboo||0), bamboo_after:nb,
      miningRate_before:user.miningRate||0, miningRate_after:newRate,
    },_meta);
    if(user.referredBy&&user.referredBy!==uid){
      const comm=Math.floor(total*G.REF_BONUS_PCT/100);
      const rr=await dbGet(env,`users/${user.referredBy}`);
      if(rr.data){
        await dbUpdate(env,`users/${user.referredBy}`,{bamboo:(rr.data.bamboo||0)+comm});
        await dbPush(env,`users/${user.referredBy}/referralEarnings`,{fromUserId:uid,amount:comm,timestamp:Date.now()});
        log(env,user.referredBy,'referral_commission',{
          fromUserId:uid, commission:comm,
          bamboo_before:(rr.data.bamboo||0), bamboo_after:(rr.data.bamboo||0)+comm,
        });
      }
    }
    return{success:true,data:{bamboo:nb,miningRate:newRate,machines}};
  }catch(e){return{success:false,error:e.message};}
}

async function hUpgradeTank(env,uid,data,_meta={}){
  try{
    const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    const cur=user.tankLevel||1;const next=cur+1;
    if(next>G.MAX_TANK_LVL)return{success:false,error:'Max level'};
    if(parseInt(data.newLevel)!==next)return{success:false,error:'Level mismatch'};
    const cost=G.TANK[next].upgCost;
    if((user.bamboo||0)<cost)return{success:false,error:'Not enough Bamboo'};
    const nb=(user.bamboo||0)-cost;
    await dbUpdate(env,`users/${uid}`,{bamboo:nb,tankLevel:next});
    log(env,uid,'upgrade_tank',{
      tankLevel_before:cur, tankLevel_after:next,
      cost, bamboo_before:(user.bamboo||0), bamboo_after:nb,
      newCap:G.TANK[next].cap,
      coins_balance:user.coins||0, miningRate:user.miningRate||0,
    },_meta);
    return{success:true,data:{tankLevel:next,bamboo:nb}};
  }catch(e){return{success:false,error:e.message};}
}

async function hExchange(env,uid,data,_meta={}){
  try{
    const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    // Only Bamboo → Coins direction allowed
    if(data.coinsAmount!==undefined)return{success:false,error:'Coins to Bamboo exchange is disabled'};
    if(data.bambooAmount===undefined)return{success:false,error:'Specify bambooAmount'};
    let nb=user.bamboo||0,nc=user.coins||0;
    const bam=Math.floor(parseInt(data.bambooAmount)||0);
    if(bam<G.BAMBOO_PER_COIN)return{success:false,error:`Min ${G.BAMBOO_PER_COIN} Bamboo`};
    if(nb<bam)return{success:false,error:'Not enough Bamboo'};
    const coins=Math.floor(bam/G.BAMBOO_PER_COIN);
    nb-=bam; nc+=coins;
    const entry={bam,coins,dir:'B→C',ts:Date.now()};
    await dbUpdate(env,`users/${uid}`,{bamboo:nb,coins:nc});
    await dbPush(env,`users/${uid}/exchHistory`,entry);
    log(env,uid,'exchange',{
      bamboo_spent:bam, coins_received:coins,
      bamboo_before:user.bamboo||0, bamboo_after:nb,
      coins_before:user.coins||0,   coins_after:nc,
    },_meta);
    return{success:true,data:{bamboo:nb,coins:nc,entry}};
  }catch(e){return{success:false,error:e.message};}
}

async function hWithdraw(env,uid,data,_meta={}){
  try{
    const addr=(data.address||'').trim();const amt=parseFloat(data.amount)||0;
    if(!addr||addr.length<10)return{success:false,error:'Invalid TON address'};
    if(amt<G.MIN_WITHDRAW)return{success:false,error:`Min ${G.MIN_WITHDRAW} Coins`};
    if(amt>1000000)return{success:false,error:'Amount too large'};
    const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    if((user.coins||0)<amt)return{success:false,error:'Not enough Coins'};
    // Partner tasks must be completed before withdrawal
    const tpr=await dbGet(env,'tasks/partner');
    const partnerTasks=tpr.data?Object.values(tpr.data).filter(t=>t.status==='active'):[];
    const completedTasks=user.completedTasks||[];
    const missingPartner=partnerTasks.filter(t=>!completedTasks.includes(t.id));
    if(missingPartner.length>0){
      return{success:false,error:'Complete all partner tasks first',errorCode:'PARTNER_TASKS_REQUIRED',missing:missingPartner.length};
    }
    // No deposit requirement — free and paid users have same withdrawal rules
    const wdId=`wd_${uid}_${Date.now()}`;const ton=amt*G.TON_PER_COIN;
    const upd={coins:(user.coins||0)-amt};
    await dbUpdate(env,`users/${uid}`,upd);
    const rec={wdId,userId:uid,address:addr,amt,ton,status:'pending',ts:Date.now()};
    await dbSet(env,`users/${uid}/wdHistory/${wdId}`,rec);
    await dbSet(env,`withdrawQueue/${wdId}`,rec);
    log(env,uid,'withdraw_request',{
      wdId, amount_coins:amt, amount_ton:ton, address:addr,
      coins_before:(user.coins||0), coins_after:upd.coins,
    },_meta);
    return{success:true,data:{wdId,coins:upd.coins,status:'pending'}};
  }catch(e){return{success:false,error:e.message};}
}

async function hDeposit(env,uid,data,_meta={}){
  try{
    const amt=parseFloat(data.amount)||0;const txHash=(data.txHash||'').slice(0,256);
    if(!txHash||amt<G.MIN_DEPOSIT_TON)return{success:false,error:'Invalid deposit data'};
    const safeHash=txHash.replace(/[^a-zA-Z0-9]/g,'_');
    const dup=await dbGet(env,`txHashes/${safeHash}`);
    if(dup.data)return{success:false,error:'Duplicate transaction'};
    const depId=`dep_${uid}_${Date.now()}`;
    const rec={depId,userId:uid,txHash,amount:amt,status:'pending',ts:Date.now()};
    const ur=await dbGet(env,`users/${uid}`);const u=ur.data||{};
    await dbSet(env,`users/${uid}/deposits/${depId}`,rec);
    await dbSet(env,`pendingDeposits/${depId}`,rec);
    await dbSet(env,`txHashes/${safeHash}`,{depId,userId:uid,ts:Date.now()});
    log(env,uid,'deposit_initiated',{
      depId, txHash, amount_ton:amt,
      bamboo_before:(u.bamboo||0), coins_before:(u.coins||0),
      tonBalance_before:(u.tonBalance||0),
    },_meta);
    // Respond immediately — balance credited within 3 minutes by server wallet monitor
    return{success:true,data:{depositId:depId,message:'Transaction registered. Your balance will be added within 3 minutes.'}};
  }catch(e){return{success:false,error:e.message};}
}

async function hVerifyDeposit(env,uid,data,_meta={}){
  try{
    const{depositId,txHash}=data;
    const dr=await dbGet(env,`users/${uid}/deposits/${depositId}`);const dep=dr.data;
    if(!dep)return{success:false,error:'Deposit not found'};
    if(dep.status==='completed')return{success:true,data:{status:'completed',amount:dep.amount}};
    try{
      const res=await fetch(`https://toncenter.com/api/v2/getTransaction?hash=${encodeURIComponent(txHash||dep.txHash)}`);
      if(res.ok){const j=await res.json();if(j.ok&&j.result){
        const tonAmt=parseFloat(dep.amount);const bamboo=Math.floor(tonAmt*G.TON_TO_BAMBOO);
        await dbUpdate(env,`users/${uid}/deposits/${depositId}`,{status:'completed',completedAt:Date.now()});
        const ur=await dbGet(env,`users/${uid}`);const u=ur.data||{};
        await dbUpdate(env,`users/${uid}`,{bamboo:(u.bamboo||0)+bamboo,tonBalance:(u.tonBalance||0)+tonAmt,hasDeposited:true});
        await dbDelete(env,`pendingDeposits/${depositId}`);
        log(env,uid,'deposit_completed',{
          depositId, txHash:txHash||dep.txHash,
          amount_ton:tonAmt, bamboo_added:bamboo,
          bamboo_before:(u.bamboo||0), bamboo_after:(u.bamboo||0)+bamboo,
          tonBalance_before:(u.tonBalance||0), tonBalance_after:(u.tonBalance||0)+tonAmt,
        },_meta);
        return{success:true,data:{status:'completed',amount:tonAmt,bambooAdded:bamboo}};
      }}
    }catch(_){}
    return{success:true,data:{status:'pending'}};
  }catch(e){return{success:false,error:e.message};}
}

async function hClaimTask(env,uid,data,_meta={}){
  try{
    const tid=data.taskId;const r=await dbGet(env,`users/${uid}`);const user=r.data;
    if(!user)return{success:false,error:'User not found'};
    if((user.completedTasks||[]).includes(tid))return{success:false,error:'Already claimed'};
    let bam=0,coins=0;
    if(G.REF_TASKS[tid]){
      const t=G.REF_TASKS[tid];
      const rr=await dbGet(env,`users/${uid}/referrals`);
      const rc=rr.data?Object.keys(rr.data).length:0;
      if(rc<t.n)return{success:false,error:`Need ${t.n} referrals (have ${rc})`};
      bam=t.bam;coins=t.coins;
    }else if(G.SOC_TASKS[tid]){bam=G.SOC_TASKS[tid];}
    else return{success:false,error:'Unknown task'};
    const nb=(user.bamboo||0)+bam;const nc=(user.coins||0)+coins;
    await dbUpdate(env,`users/${uid}`,{completedTasks:[...(user.completedTasks||[]),tid],bamboo:nb,coins:nc});
    log(env,uid,'claim_task',{
      taskId:tid,
      bamboo_reward:bam, coins_reward:coins,
      bamboo_before:(user.bamboo||0), bamboo_after:nb,
      coins_before:(user.coins||0),   coins_after:nc,
    },_meta);
    return{success:true,data:{bamboo:nb,coins:nc,bam,coins}};
  }catch(e){return{success:false,error:e.message};}
}

async function hAdmin(env,action,data){
  switch(action){
    case 'adminGetUser':{const r=await dbGet(env,`users/${data.userId}`);return{success:true,data:r.data||null};}
    case 'adminSetBalance':{
      const r=await dbGet(env,`users/${data.userId}`);if(!r.data)return{success:false,error:'Not found'};
      const u={};
      if(data.bamboo!==undefined)u.bamboo=Math.max(0,parseFloat(data.bamboo));
      if(data.coins!==undefined)u.coins=Math.max(0,parseFloat(data.coins));
      if(data.tonBalance!==undefined)u.tonBalance=Math.max(0,parseFloat(data.tonBalance));
      await dbUpdate(env,`users/${data.userId}`,u);
      log(env,data.userId,'admin_set_balance',{
        bamboo_set:data.bamboo, coins_set:data.coins, ton_set:data.tonBalance,
        bamboo_before:r.data.bamboo||0, coins_before:r.data.coins||0,
        by:'admin',
      });
      return{success:true};
    }
    case 'adminConfirmDeposit':{
      const dep=await dbGet(env,`users/${data.userId}/deposits/${data.depositId}`);
      if(!dep.data)return{success:false,error:'Not found'};
      const ton=parseFloat(data.amount||dep.data.amount);const bamboo=Math.floor(ton*G.TON_TO_BAMBOO);
      await dbUpdate(env,`users/${data.userId}/deposits/${data.depositId}`,{status:'completed',completedAt:Date.now()});
      const u=await dbGet(env,`users/${data.userId}`);
      if(u.data)await dbUpdate(env,`users/${data.userId}`,{bamboo:(u.data.bamboo||0)+bamboo,tonBalance:(u.data.tonBalance||0)+ton,hasDeposited:true});
      await dbDelete(env,`pendingDeposits/${data.depositId}`);
      log(env,data.userId,'admin_confirm_deposit',{
        depositId:data.depositId, amount_ton:ton, bamboo_added:bamboo, by:'admin',
      });
      return{success:true,data:{bambooAdded:bamboo}};
    }
    case 'adminApproveWithdraw':{
      await dbUpdate(env,`users/${data.userId}/wdHistory/${data.wdId}`,{status:'approved',approvedAt:Date.now()});
      await dbDelete(env,`withdrawQueue/${data.wdId}`);
      log(env,data.userId,'admin_approve_withdraw',{wdId:data.wdId,by:'admin'});
      return{success:true};
    }
    case 'adminRejectWithdraw':{
      const wd=await dbGet(env,`users/${data.userId}/wdHistory/${data.wdId}`);
      if(!wd.data)return{success:false,error:'Not found'};
      await dbUpdate(env,`users/${data.userId}/wdHistory/${data.wdId}`,{status:'rejected',rejectedAt:Date.now()});
      if(data.refund){const u=await dbGet(env,`users/${data.userId}`);if(u.data)await dbUpdate(env,`users/${data.userId}`,{coins:(u.data.coins||0)+(wd.data.amt||0)});}
      await dbDelete(env,`withdrawQueue/${data.wdId}`);
      log(env,data.userId,'admin_reject_withdraw',{wdId:data.wdId,refund:!!data.refund,amount:wd.data?.amt||0,by:'admin'});
      return{success:true};
    }
    case 'adminGetQueue':{
      const w=await dbGet(env,'withdrawQueue');const d=await dbGet(env,'pendingDeposits');
      return{success:true,data:{withdrawals:w.data?Object.values(w.data):[],deposits:d.data?Object.values(d.data):[]}};
    }
    default:return{success:false,error:'Unknown admin action'};
  }
}

// ── Check Telegram channel membership ────────────────────────────
async function checkMembership(env,userId,channelLink){
  try{
    if(!env.BOT_TOKEN){console.log('No BOT_TOKEN, skipping check');return true;}
    let username=channelLink;
    if(channelLink.includes('t.me/')) username=channelLink.split('t.me/')[1].split('?')[0].split('/')[0];
    if(username.startsWith('@')) username=username.substring(1);
    const res=await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getChatMember`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({chat_id:`@${username}`,user_id:parseInt(userId)}),
    });
    const j=await res.json();
    if(!j.ok){console.error('TG API:',j);return false;}
    return['member','administrator','creator'].includes(j.result?.status);
  }catch(e){console.error('checkMembership:',e.message);return false;}
}

// ── Verify Task ───────────────────────────────────────────────────
async function hVerifyTask(env,uid,data,_meta={}){
  try{
    const{taskId,taskType,taskCategory}=data;
    if(!taskId||typeof taskId!=='string'||taskId.length>100) return{success:false,error:'Invalid taskId'};
    const cat=taskCategory||'community';
    // Find task
    let tr=await dbGet(env,`tasks/${cat}/${taskId}`);
    let task=tr.data, taskCat=cat;
    if(!task){
      const other=cat==='community'?'partner':'community';
      tr=await dbGet(env,`tasks/${other}/${taskId}`);
      task=tr.data; taskCat=other;
    }
    if(!task)return{success:false,error:'Task not found'};
    if(task.status!=='active')return{success:false,error:'Task is no longer active'};
    // Fix 1: server-side double-claim guard — check user's completedTasks first
    const ur=await dbGet(env,`users/${uid}`);const u=ur.data||{};
    if((u.completedTasks||[]).includes(taskId))return{success:false,error:'Task already completed'};
    if((task.completedBy||[]).includes(uid))return{success:false,error:'Task already completed'};
    // Channel: verify membership
    if(task.type==='channel'){
      const isMember=await checkMembership(env,uid,task.link);
      if(!isMember)return{success:false,error:'Not a member of the channel. Join first then try again!'};
    }
    const bam=task.bambooReward||500;
    const newCompletions=(task.completions||0)+1;
    const newCompletedBy=[...(task.completedBy||[]),uid];
    const taskUpdates={completions:newCompletions,completedBy:newCompletedBy,updatedAt:Date.now()};
    if(newCompletions>=(task.targetUsers||Infinity)) taskUpdates.status='completed';
    await dbUpdate(env,`tasks/${taskCat}/${taskId}`,taskUpdates);
    // Fix 1: mark completed in user BEFORE bamboo so duplicate calls return "already completed"
    const newCompleted=[...(u.completedTasks||[]),taskId];
    await dbUpdate(env,`users/${uid}`,{
      completedTasks:newCompleted,
      bamboo:(u.bamboo||0)+bam,
    });
    log(env,uid,'verify_task',{
      taskId, taskType:task.type, taskCategory:taskCat,
      bamboo_reward:bam,
      bamboo_before:(u.bamboo||0), bamboo_after:(u.bamboo||0)+bam,
    },_meta);
    return{success:true,data:{bambooAdded:bam,completions:newCompletions}};
  }catch(e){console.error('verifyTask:',e);return{success:false,error:e.message};}
}

// ── Create Task ───────────────────────────────────────────────────
async function hCreateTask(env,uid,data,_meta={}){
  try{
    const{type,link,targetUsers}=data;
    if(!['channel','bot'].includes(type)) return{success:false,error:'Invalid type. Must be channel or bot'};
    const target=parseInt(targetUsers)||0;
    if(target<100)  return{success:false,error:'Minimum target is 100 users'};
    if(target>100000) return{success:false,error:'Maximum target is 100,000 users'};
    if(!link||!link.includes('t.me/')) return{success:false,error:'Valid Telegram link required'};
    // 60 Coins per target user → 100 users = 6,000 | 500 users = 30,000
    const COINS_PER_USER=60;
    const cost=target*COINS_PER_USER;
    const ur=await dbGet(env,`users/${uid}`);const u=ur.data;
    if(!u) return{success:false,error:'User not found'};
    if((u.coins||0)<cost) return{success:false,error:`Insufficient Coins. Need ${cost} Coins`};
    // Deduct Coins
    await dbUpdate(env,`users/${uid}`,{coins:(u.coins||0)-cost});
    // Extract display name from link
    const username=link.split('t.me/')[1]?.split('?')[0]?.split('/')[0]||link;
    const now=Date.now();
    const taskId=`task_${now}_${Math.random().toString(36).substring(2,10)}`;
    const taskData={
      id:taskId, creatorId:uid, type, link,
      name:`@${username}`,
      targetUsers:target,
      bambooReward:500,
      completions:0, completedBy:[],
      status:'active',
      createdAt:now,
      expiresAt:now+(30*24*60*60*1000),
      updatedAt:now,
    };
    await dbSet(env,`tasks/community/${taskId}`,taskData);
    await dbPush(env,`users/${uid}/transactions`,{type:'create_task',taskId,taskType:type,targetUsers:target,cost,coinsCost:cost,timestamp:now});
    log(env,uid,'create_task',{
      taskId, taskType:type, targetUsers:target,
      coins_spent:cost,
      coins_before:(u.coins||0)+cost, coins_after:(u.coins||0),
      taskLink:link,
    },_meta);
    return{success:true,data:{taskId,type,targetUsers:target,totalCost:cost,bambooReward:500}};
  }catch(e){console.error('createTask:',e);return{success:false,error:e.message};}
}

// ── Main handler ──────────────────────────────────────────────────
export default {
  async fetch(request,env){
    if(request.method==='OPTIONS')return new Response(null,{headers:CORS});
    const url=new URL(request.url);const path=url.pathname;
    if(path==='/health')return ok({status:'ok',ts:Date.now(),env:env.ENVIRONMENT||'production'});
    if(path==='/tonconnect-manifest.json')return jRes({url:'https://YOUR_FRONTEND_URL',name:'PandaBambooBot',iconUrl:'https://i.supaimg.com/ec27537b-aa6a-42cf-8ba1-d6850eeea36d/d7e19f8c-7876-4dc6-8542-d4f615704e46.png',description:'Panda Bamboo Factory'});
    if(path!=='/api'||request.method!=='POST')return fail('Not found',404);

    const ip=request.headers.get('CF-Connecting-IP')||'unknown';
    if(!rateOk(ip))return fail('Rate limit exceeded',429);

    let body;
    try{const raw=await request.text();if(raw.length>10240)return fail('Payload too large',413);body=JSON.parse(sanitise(raw));}
    catch(_){return fail('Invalid JSON',400);}

    const authHeader=request.headers.get('Authorization')||'';
    const action=request.headers.get('X-Action')||body.action;
    const data=body.data||{};
    if(!action)return fail('Missing action',400);

    const ADMIN_ACTIONS=new Set(['adminGetUser','adminSetBalance','adminConfirmDeposit','adminApproveWithdraw','adminRejectWithdraw','adminGetQueue']);
    if(ADMIN_ACTIONS.has(action)){
      const v=await validateTg(authHeader.replace('Telegram ',''),env.BOT_TOKEN);
      if(!v.valid)return fail('Unauthorized',401);
      const adminIds=(env.ADMIN_IDS||'').split(',').map(s=>s.trim());
      if(!adminIds.includes(String(v.user?.id)))return fail('Forbidden',403);
      return jRes(await hAdmin(env,action,data));
    }

    if(!authHeader.startsWith('Telegram '))return fail('Telegram authentication required',401);
    const v=await validateTg(authHeader.replace('Telegram ',''),env.BOT_TOKEN);
    if(!v.valid){
      console.error('TG validation failed:',v.error);
      return jRes({success:false,error:'Invalid Telegram authentication',errorCode:'INVALID_TELEGRAM_AUTH',debug:{hasInitData:!!authHeader,botTokenConfigured:!!env.BOT_TOKEN,environment:env.ENVIRONMENT||'production',validationError:v.error}},401);
    }

    const uid=String(v.user.id);
    const _meta={ip, ua:request.headers.get('User-Agent')||''};
    console.log(`[${new Date().toISOString()}] User:${uid} Action:${action} IP:${ip}`);

    switch(action){
      case 'getState'      :return jRes(await hGetState(env,uid,v.user,{...data,_startParam:v.startParam||''},_meta));
      case 'collect'       :return jRes(await hCollect      (env,uid,data,_meta));
      case 'buyItem'       :return jRes(await hBuyItem      (env,uid,data,_meta));
      case 'upgradeTank'   :return jRes(await hUpgradeTank  (env,uid,data,_meta));
      case 'exchange'      :return jRes(await hExchange     (env,uid,data,_meta));
      case 'withdraw'      :return jRes(await hWithdraw     (env,uid,data,_meta));
      case 'deposit'       :return jRes(await hDeposit      (env,uid,data,_meta));
      case 'verifyDeposit' :return jRes(await hVerifyDeposit(env,uid,data,_meta));
      case 'claimTask'     :return jRes(await hClaimTask    (env,uid,data,_meta));
      case 'verifyTask'    :return jRes(await hVerifyTask   (env,uid,data,_meta));
      case 'createTask'    :return jRes(await hCreateTask   (env,uid,data,_meta));
      default:return fail('Unknown action',400);
    }
  }
};
