// index.js — MetroFitness Bot (Telegraf)
// ВЕРСИЯ 15.11.2025 — правки: уведомления «я уже клиент», чистый текст без рамок,
// фикс «Получить и обменять Метрики», перенос инлайн-кнопок подтверждения к заявке,
// подтверждение реферала и клиента менеджером, фикс меню и ссылки.
// Требуемые пакеты: telegraf, dotenv, qrcode (опционально)

const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');

let QRCode = null;
try { QRCode = require('qrcode'); } catch(e){ console.log('QR отключён (npm i qrcode)'); }

const bot = new Telegraf(process.env.BOT_TOKEN || '');
const ADMIN_IDS = String(process.env.ADMIN_IDS || '').split(',').map(s=>s.trim()).filter(Boolean);
const isAdmin = (ctx)=> ADMIN_IDS.includes(String(ctx.from?.id));

async function tgRetry(fn, tries=4, delay=400){
  let last;
  for (let i=0;i<tries;i++){
    try{ return await fn(); }catch(e){
      last=e; const m=String(e?.message||'');
      if (/ECONNRESET|ETIMEDOUT|EAI_AGAIN|429/i.test(m)) { await new Promise(r=>setTimeout(r, delay*(i+1))); continue; }
      break;
    }
  }
  throw last;
}
const tgSend={
  text:(id,t,extra)=>tgRetry(()=>bot.telegram.sendMessage(id,t,extra)),
  photo:(id,f,extra)=>tgRetry(()=>bot.telegram.sendPhoto(id,f,extra)),
};

let BOT_USERNAME = process.env.BOT_USERNAME || null;
(async()=>{ try{ const me=await bot.telegram.getMe(); BOT_USERNAME = me.username; }catch(_){}})();
const buildRefLink = (code)=>'https://t.me/'+(BOT_USERNAME||bot?.botInfo?.username||'your_bot_username')+'?start='+code;
const normCode = (v)=> String(v||'').trim().toLowerCase();
function normPhone(v){
  const s=String(v||'').replace(/[^\d+]/g,'');
  if (s.startsWith('+')) return s.replace(/^\+8/,'+7').replace(/^\+7/,'+7');
  if (s.startsWith('8')) return '+7'+s.slice(1);
  if (s.startsWith('7')) return '+7'+s.slice(1);
  return s||'';
}

/** Уведомление пригласителю о начислении за покупку друга
 *  Отправляет сообщение в стиле модерации активностей:
 *  "+10 метриков" и "Текущие: X (Статус)".
 */
// ===== notifyReferrerAboutAward (замена) =====
async function notifyReferrerAboutAward(db, { referrer_id, refcode, invitee_id }, ctx){
  // найти пригласителя: по id или по refcode
  let ref = null;
  if (referrer_id) ref = db.members[referrer_id] || null;
  if (!ref && refcode) {
    ref = Object.values(db.members).find(u => normCode(u.ref_code) === normCode(refcode)) || null;
  }
  if (!ref) {
    try { await tgSend.text(ctx.chat.id, '⚠️ Пригласитель не найден в БД.'); } catch(_) {}
    return { ok:false, reason:'referrer_not_found' };
  }

  // кто купил
  const inv = db.members[invitee_id] || {};
  const who = inv.username ? '@'+inv.username : `id ${inv.id||'?'}`;

  // баллы за покупку
  const add = (ACTIVITIES.friend_purchase?.points ?? 10);

  // на этом этапе очки уже пересчитаны выше (recalcAndSave)
  const pointsNow = ref.points || 0;
  const tierNow   = ref.tier   || 'Нет статуса';

  const text =
    `🎉 Подтверждена покупка друга: ${who}\n` +
    `+${add} метрик(ов)\n` +
    `Текущие: ${pointsNow} (${tierNow})`;

  try {
    await tgSend.text(ref.id, text);
    return { ok:true };
  } catch (e) {
    // фолбэк менеджеру, чтобы было видно, почему не доставили
    try { await tgSend.text(ctx.chat.id, `⚠️ Не удалось отправить уведомление пригласителю (${ref.id}): ${e.message||e}`); } catch(_){}
    return { ok:false, reason:'telegram_send_error' };
  }
}


// [FIX-2] простой формат текста без рамок
function box(title, lines){
  const body = Array.isArray(lines) ? lines.join('\n') : String(lines||'');
  return (title ? `*${title}*\n` : '') + body;
}
async function sendCard(ctx, title, bodyLines, kb, assetKey){
  const db = loadDB();
  const text = box(title, bodyLines);
  const fid = assetKey ? db.assets[assetKey] : null;
  if (fid){
    try{ return tgSend.photo(ctx.chat.id, fid, Object.assign({caption:text, parse_mode:'Markdown'}, kb||{})); }
    catch{ /*fallback*/ }
  }
  return tgSend.text(ctx.chat.id, text, Object.assign({parse_mode:'Markdown'}, kb||{}));
}

// [NEW] показать сохранённый баннер, если есть — для вызовов без ошибок
async function sendAssetIfExists(ctx, assetKey) {
  const db = loadDB();
  const fid = db.assets?.[assetKey];
  if (!fid) return;
  try { await tgSend.photo(ctx.chat.id, fid); } catch (_) {}
}
// Безопасный Markdown
const mdEsc = (s)=> String(s||'').replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g,'\\$1');
// Красивое упоминание пользователя
const userTag = (u)=> u?.username ? ('@'+u.username) : ('id '+(u?.id||'?'));

// DB
const DB_FILE = path.resolve(__dirname, 'db.json');
function ensureDefaults(db){
  if (!db || typeof db!=='object') db={};
  db.members ||= {};
  db.referrals ||= []; // {referrer_code, invitee_id, stage, phone, ts}
  db.news ||= [];
  db.schedules ||= {};
  db.promos ||= {};
  db.events ||= [];
  db.config ||= {};
  db.assets ||= {};
  db.activity_submissions ||= [];
  db.qr_cache ||= {};
  db.pending_purchase ||= {};    // состояние проверки покупки для менеджеров
  db.config.next_submission_id ||= 1;
  db.config.last_reminders_run ||= 0;
  return db;
}
function loadDB(){ try{ return ensureDefaults(JSON.parse(fs.readFileSync(DB_FILE,'utf8')));}catch{ return ensureDefaults({}); } }
function saveDB(db){ fs.writeFileSync(DB_FILE, JSON.stringify(ensureDefaults(db), null, 2)); }
const nextSubmissionId = (db)=> (db.config.next_submission_id = (db.config.next_submission_id||1)+1) - 1;

// UI state
const makeRefCode = (tgId)=> crypto.createHash('sha1').update(String(tgId)).digest('hex').slice(0,8);
function ensureMember(db, from){
  if (!db.members[from.id]){
    db.members[from.id] = {
      id: from.id, username: from.username||null, first_name: from.first_name||null,
      phone:null, ref_code: makeRefCode(from.id), referred_by:null,
      ui:'MAIN', ui_prev:'MAIN', points:0, tier:'Нет статуса',
      tmp_promo:null, bcast_draft:null, ref_last_shared_ts:0,
      is_client:false, contract_no:null
    };
  } else {
    if (from.username) db.members[from.id].username=from.username;
    if (from.first_name) db.members[from.id].first_name=from.first_name;
    db.members[from.id].points ||= 0;
    db.members[from.id].tier ||= 'Нет статуса';
    db.members[from.id].ui_prev ??= 'MAIN';
    db.members[from.id].tmp_promo ??= null;
    db.members[from.id].bcast_draft ??= null;
    db.members[from.id].ref_last_shared_ts ??= 0;
    db.members[from.id].is_client ??= false;
    db.members[from.id].contract_no ??= null;
  }
  return db.members[from.id];
}
function setUI(db, uid, ui, opts={}){
  const m=ensureMember(db, {id:uid});
  if (!opts.noHistory) m.ui_prev = m.ui || 'MAIN';
  m.ui = ui;
  saveDB(db);
}
function goBack(db, uid){
  const m=ensureMember(db,{id:uid});
  const to=m.ui_prev||'MAIN';
  m.ui=to; m.ui_prev='MAIN'; saveDB(db);
  return to;
}

// Program/points
const ACTIVITIES = {
  review_2gis:{title:'Отзыв 2ГИС',points:1},
  review_yandex:{title:'Отзыв Яндекс',points:1},
  story_vk:{title:'История ВК',points:1},
  status_wa:{title:'Статус WhatsApp',points:1},
  story_tg:{title:'История Telegram',points:1},
  friend_purchase:{title:'Покупка приглашённого друга',points:10},
};
const TIERS = [
  {name:'Бронза',threshold:15,reward:'1,5 месяца к абонементу'},
  {name:'Серебро',threshold:35,reward:'4 месяца или брендированные носки'},
  {name:'Золото',threshold:50,reward:'6 месяцев или брендированное полотенце'},
  {name:'Платина',threshold:70,reward:'8 мес + 1 мес заморозки или фирменный шейкер и футболка'},
  {name:'Бриллиант',threshold:100,reward:'12 мес + 1,5 мес заморозки или бренд-пак'},
  {name:'Легенда',threshold:200,reward:'Амбассадор бренда (детали у Руководителя)'},
];
const tierForPoints = (pts)=>{ let cur={name:'Нет статуса',threshold:0,reward:'—'}; for(const t of TIERS){ if(pts>=t.threshold) cur=t; } return cur; };
const nextTierInfo = (pts)=> TIERS.find(t=>pts<t.threshold)||null;
const progressBar = (x,t)=> t?`▰`.repeat(Math.floor(10*Math.min(1,x/t)))+`▱`.repeat(10-Math.floor(10*Math.min(1,x/t)))+`  ${x}/${t}`:'';

function calcPoints(db, uid){
  const me=db.members[uid]; if(!me) return 0;
  const my=normCode(me.ref_code);
  let total=0;
  total += db.referrals.filter(r=>String(r?.stage).trim()==='purchased' && normCode(r?.referrer_code)===my).length * (ACTIVITIES.friend_purchase.points||10);
  for (const a of db.activity_submissions.filter(a=>a.user_id===uid && a.status==='approved')) total+= (ACTIVITIES[a.type]?.points||0);
  return total;
}
const recalcUser=(db,u)=>{ u.points=calcPoints(db,u.id); u.tier=tierForPoints(u.points).name; };
const recalcAndSave=(db,u)=>{ recalcUser(db,u); saveDB(db); };

// Links
function askChannelOrLink(){
  const tg='https://t.me/metrofitness_msk', vk='https://vk.com/metrofitnessmsk';
  return Markup.inlineKeyboard([
    [Markup.button.url('📣 Наш Telegram-канал', tg)],
    [Markup.button.url('VK MetroFitness', vk)]
  ]);
}

// Feedback links
const FEEDBACK_URLS={
  borisovskiye:{ dgis:'https://2gis.ru/moscow/branches/70000001100646682/firm/70000001100646683/37.757944%2C55.640271?m=37.738772%2C55.703422%2F11.24', yandex:'https://yandex.ru/maps/-/CLCDmDkx'},
  profsoyuznaya:{ dgis:'https://2gis.ru/moscow/branches/70000001100646682/firm/70000001100646722/37.521318%2C55.636578?m=37.738772%2C55.703422%2F11.24', yandex:'https://yandex.ru/maps/-/CLCDmPmQ'}
};
const clubKeyByTitleFeedback=(t)=> t.includes('Ключевой')?'borisovskiye' : t.includes('Дропин')?'profsoyuznaya' : null;

// Keyboards
function mainMenuFor(ctx){
  const db = loadDB();
  const m  = ensureMember(db, ctx.from);

  // Базовые ряды
  let rows = [
    ['Хочу абонемент', 'Мой статус и Метрики'],
    ['Моя реферальная ссылка', 'Новости клуба'],
    ['Задать вопрос', 'Оставить отзыв']
  ];

  // Если клиент подтверждён — убираем «Хочу абонемент» и оставляем ОДНУ кнопку «Моя реферальная ссылка»
  if (m.is_client) {
    rows = [
      ['Мой статус и Метрики', 'Новости клуба'],
      ['Задать вопрос', 'Оставить отзыв'],
      ['Моя реферальная ссылка']
    ];
  } else {
    // Показываем «Я уже клиент» только тем, кто ещё НЕ подтверждён
    rows.push(['Я уже клиент']);
  }

  if (isAdmin(ctx)) rows.push(['Меню менеджера']);
  return Markup.keyboard(rows).resize();
}

const wantMenu=()=>Markup.keyboard([['Узнать условия у менеджера','Приобрести абонемент'],['Назад']]).resize();
const wantContactMenu=()=>Markup.keyboard([[Markup.button.contactRequest('📲 Отправить номер')],['Назад']]).resize();
const statusMenu=()=>Markup.keyboard([['Получить и обменять Метрики','Акции для друзей'],['Программа лояльности'],['Назад']]).resize();
const metricsMainMenu=()=>Markup.keyboard([['Проверить баллы и статус','Проверить Метрики за активность'],['Обменять Метрики','Программа лояльности'],['Назад']]).resize();
const activitiesMenu=()=>Markup.keyboard([['Отзыв 2ГИС','Отзыв Яндекс'],['История ВК','Статус WA'],['История Telegram'],['Назад']]).resize();
const promosMenuInsideStatus=()=>Markup.keyboard([['Трейд-ин','День рождения','Студент'],['Назад']]).resize();
const newsMenu=()=>Markup.keyboard([['Что нового?','Расписание групповых тренировок'],['Когда ближайшие соревнования'],['Назад']]).resize();
const scheduleClubsMenu=()=>Markup.keyboard([['Дропин','Ключевой'],['Назад']]).resize();
const supportMenu=()=>Markup.keyboard([['Менеджер','Тренер'],['Назад']]).resize();
const supportManagerMenu=()=>Markup.keyboard([['Удобно в чате','Удобно принять звонок'],['Назад']]).resize();
const supportManagerPhoneMenu=()=>Markup.keyboard([[Markup.button.contactRequest('📲 Отправить номер')],['Назад']]).resize();
const supportCoachMenu=()=>Markup.keyboard([['Составить план питания','Записаться на тренировку'],['Хочу блок тренировок'],['Другой вопрос'],['Назад']]).resize();
const coachPackMenu=()=>Markup.keyboard([['1 Тренировка - 3 500','10 Тренировка - 22 000'],['15 Тренировка - 30 000','20 Тренировка - 38 000'],['Назад']]).resize();
const feedbackClubsMenu=()=>Markup.keyboard([['Клуб на Борисовских Прудах, 26 — ТРЦ «Ключевой»'],['Клуб на Профсоюзной, 118 — ТЦ «Дропин»'],['Назад']]).resize();
const adminMenu=()=>Markup.keyboard([['Заявки на активности'],['Отметить покупку'],['Рассылка всем','Добавить расписание'],['Назад']]).resize();
const adminAddScheduleMenu=()=>Markup.keyboard([['Клуб на Профсоюзной, 118 — ТЦ «Дропин»'],['Клуб на Борисовских Прудах, 26 — ТРЦ «Ключевой»'],['Назад']]).resize();

// UI rendering
async function showUI(ctx, ui){
  switch(ui){
    case 'MAIN': return sendCard(ctx,'Главное меню','Полезные ссылки:', mainMenuFor(ctx),'banner_main');
    case 'WANT': return sendCard(ctx,'Хочу абонемент','Выбирайте действие:', wantMenu(),'banner_want');
    case 'STATUS': return sendCard(ctx,'Мой статус и Метрики','Выберите раздел:', statusMenu(),'banner_bonuses');
    case 'METRICS_MAIN': return sendCard(ctx,'Метрики','Что сделаем?', metricsMainMenu(),'banner_activity');
    case 'ACTIVITY_MENU': return sendCard(ctx,'Подтверждение активностей','Выберите тип:', activitiesMenu(),'banner_activity');
    case 'PROMOS_IN_STATUS': return sendCard(ctx,'Акции','Выберите акцию:', promosMenuInsideStatus(),'banner_promos');
    case 'NEWS': return sendCard(ctx,'Новости и расписание','Выберите:', newsMenu(),'banner_news');
    case 'SCHEDULE': return sendCard(ctx,'Расписание','Выберите клуб:', scheduleClubsMenu());
    case 'SUPPORT': return sendCard(ctx,'Поддержка','Кому задать вопрос?', supportMenu(),'banner_support');
    case 'SUPPORT_MANAGER': return sendCard(ctx,'Менеджер','Как связаться?', supportManagerMenu(),'banner_support');
    case 'SUPPORT_MANAGER_PHONE': return sendCard(ctx,'Менеджер','Нажмите кнопку — отправьте номер', supportManagerPhoneMenu(),'banner_support');
    case 'SUPPORT_COACH': return sendCard(ctx,'Тренер','Что нужно?', supportCoachMenu(),'banner_support');
    case 'FEEDBACK': return sendCard(ctx,'Оставить отзыв','Выберите клуб:', feedbackClubsMenu(),'banner_feedback');
    default: return sendCard(ctx,'Главное меню','Полезные ссылки:', mainMenuFor(ctx),'banner_main');
  }
}

bot.catch(async (err)=>{
  console.error('Telegraf error:', err);
  try{ await tgSend.text(ADMIN_IDS[0], '⚠️ Ошибка бота: '+(err.message||String(err))); }catch(_){}
});

// Content/admin helpers
bot.command('asset_set', (ctx)=>{ if(!isAdmin(ctx)) return; const k=ctx.message.text.replace('/asset_set','').trim(); if(!k) return tgSend.text(ctx.chat.id,'/asset_set <ключ>'); const db=loadDB(); db.config.pending_asset_key=k; saveDB(db); tgSend.text(ctx.chat.id,'Ок, жду фото для: '+k); });
bot.on('photo', async (ctx,next)=>{ const db=loadDB(); if(isAdmin(ctx)&&db.config.pending_asset_key){ const best=ctx.message.photo.at(-1); db.assets[db.config.pending_asset_key]=best.file_id; db.config.pending_asset_key=null; saveDB(db); return; } return next(); });

bot.command('news_add', (ctx)=>{ if(!isAdmin(ctx)) return; const t=ctx.message.text.replace('/news_add','').trim(); if(!t) return; const db=loadDB(); db.news.push({ts:Date.now(),text:t}); saveDB(db); tgSend.text(ctx.chat.id,'Новость добавлена ✅'); });
bot.command('news_clear', (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); db.news=[]; saveDB(db); tgSend.text(ctx.chat.id,'Новости очищены ✅'); });

bot.command('recalc_all', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); Object.values(db.members).forEach(u=>recalcUser(db,u)); saveDB(db); tgSend.text(ctx.chat.id,'Пересчитано ✅'); });
bot.command('recalc_me', (ctx)=>{ const db=loadDB(); const m=db.members[ctx.from.id]; if(!m) return tgSend.text(ctx.chat.id,'Нет записи о вас.'); recalcAndSave(db,m); tgSend.text(ctx.chat.id,`Метрики: ${m.points}, статус: ${m.tier}`); });

// START
bot.start(async (ctx)=>{
  const db=loadDB();
  const payload=(ctx.startPayload||'').trim();
  const me=ensureMember(db, ctx.from);

  if (payload){
    // зафиксировать переход
    const existed=db.referrals.find(r=>r.referrer_code===payload && r.invitee_id===ctx.from.id && r.stage==='started');
    if(!existed){ db.referrals.push({referrer_code:payload, invitee_id:ctx.from.id, stage:'started', phone:null, ts:Date.now()}); saveDB(db); }
    me.referred_by=payload; saveDB(db);

    // уведомление о новом переходе — БЕЗ кнопок
    const inviter = Object.values(db.members).find(m => normCode(m.ref_code)===normCode(payload));
    const note = `👥 Новый переход по реферальной ссылке\nПригласитель: @${inviter?.username||'-'} (${inviter?.id||'?'})\nНовый клиент: @${me.username||'-'} (${me.id})`;
    for (const id of ADMIN_IDS){ try{ await tgSend.text(id, note); }catch(_){ } }
  }

  setUI(db, me.id, 'MAIN', {noHistory:true});
  await sendCard(ctx,'MetroFitness',[
    'Реферальная программа и Метрики.',
    'Приводи друзей, набирай Метрики и получай статусы.'
  ], mainMenuFor(ctx),'banner_main');
});
bot.hears('Назад', async (ctx)=>{ const db=loadDB(); const to=goBack(db, ctx.from.id); await showUI(ctx,to); });

// Хочу абонемент
bot.hears('Хочу абонемент', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'WANT'); await showUI(ctx,'WANT'); });
bot.hears('Узнать условия у менеджера', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'WANT_CONTACT'); await showUI(ctx,'SUPPORT_MANAGER'); });
bot.hears('Удобно в чате', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  if (!['WANT_CONTACT','SUPPORT_MANAGER'].includes(m.ui)) return;
  await Promise.all(ADMIN_IDS.map(id=>tgSend.text(id, `💬 Запрос в чат\nИмя: ${m.first_name||''}\nUser: @${m.username||'-'}\nID: ${m.id}`)));
  await sendCard(ctx,'Спасибо!','В течение 10 минут менеджер свяжется.', mainMenuFor(ctx),'banner_support');
  setUI(db, m.id, 'MAIN', {noHistory:true});
});
bot.hears('Удобно принять звонок', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  if (m.ui==='WANT_CONTACT'){ setUI(db,m.id,'WANT_PHONE'); return sendCard(ctx,'Отправьте контакт','Нажмите кнопку, чтобы отправить номер', wantContactMenu(),'banner_support'); }
  if (m.ui==='SUPPORT_MANAGER'){ setUI(db,m.id,'SUPPORT_MANAGER_PHONE'); return sendCard(ctx,'Отправьте контакт','Нажмите кнопку, чтобы отправить номер', supportManagerPhoneMenu(),'banner_support'); }
});
bot.hears('Приобрести абонемент', async (ctx)=>{
  const db=loadDB(); setUI(db, ctx.from.id,'BUY_FORM');
  await sendCard(ctx,'Оформление абонемента',['Пришлите одним сообщением:','ФИО; дата рождения (ДД.ММ.ГГГГ); номер телефона'], Markup.keyboard([['Назад']]).resize(),'banner_wwant');
});

// «Я уже клиент» — уведомление менеджеру + подтверждение
bot.hears('Я уже клиент', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  setUI(db, m.id,'I_AM_CLIENT');
  const kb=Markup.keyboard([['Подтвердить по телефону','Подтвердить по номеру договора'],['Назад']]).resize();
  await sendCard(ctx,'Подтверждение статуса','Выберите способ:', kb,'banner_main');
});
bot.hears('Подтвердить по телефону', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  setUI(db, m.id, 'I_AM_CLIENT_PHONE_WAIT');
  await sendCard(ctx,'Отправьте контакт','Нажмите кнопку, чтобы передать номер', wantContactMenu(),'banner_main');
});
bot.hears('Подтвердить по номеру договора', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  setUI(db, m.id, 'I_AM_CLIENT_CONTRACT');
  await sendCard(ctx,'Номер договора','Пришлите номер договора одним сообщением', Markup.keyboard([['Назад']]).resize(),'banner_main');
});

// Реферальная ссылка (СТАБИЛЬНО — plain text)
async function sendReferralFast(ctx){
  const db=loadDB(); const me=ensureMember(db, ctx.from);
  const link=buildRefLink(me.ref_code);
  me.ref_last_shared_ts=Date.now(); saveDB(db);

  await bot.telegram.sendMessage(
    ctx.chat.id,
    'Твоя реферальная ссылка:\n' + link,
    Markup.inlineKeyboard([
      [Markup.button.url('Поделиться в Telegram','https://t.me/share/url?url='+encodeURIComponent(link))]
    ])
  );

  const cached=db.qr_cache[me.ref_code];
  if (cached){
    try{ await tgSend.photo(ctx.chat.id, cached, {caption:'QR для офлайна и сторис'}); }
    catch(_){ const db2=loadDB(); delete db2.qr_cache[me.ref_code]; saveDB(db2); }
    return;
  }
  setTimeout(async()=>{
    try{
      if(!QRCode) return;
      const buf=await QRCode.toBuffer(link,{width:320,margin:1,errorCorrectionLevel:'L'});
      const sent=await bot.telegram.sendPhoto(ctx.chat.id,{source:buf},{caption:'QR для офлайна и сторис'});
      const best=(sent.photo||[]).at(-1);
      if(best?.file_id){ const db2=loadDB(); db2.qr_cache[me.ref_code]=best.file_id; saveDB(db2); }
    }catch(_){}
  },0);
}
bot.hears('Моя реферальная ссылка', sendReferralFast);

// Статус и Метрики
bot.hears('Мой статус и Метрики', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'STATUS'); await showUI(ctx,'STATUS'); });
function programText(){
  return [
    '⭐️ Программа лояльности MetroFitness',
    '────────────────────────────',
    'Метрик 🌕 — внутренняя валюта сети спорт-клубов MetroFitness.',
    'Зарабатывай Метрики: выполняй активности, приглашай друзей и участвуй в клубных соревнованиях.',
    '',
    'Активности:',
    '• 🌚 Отзыв в 2ГИС — 1 метрик',
    '• 🌝 Отзыв в Яндекс Картах — 1 метрик',
    '• 🌚 История в ВК — 1 метрик',
    '• 🌝 Статус в WhatsApp — 1 метрик',
    '• 🌚 История в Telegram — 1 метрик',
    '• 🌝 Участие в соревнованиях — 5 метриков',
    '• 🌚 Пригласить друга — 10 метриков',
    '',
    'Обмен наград:',
    '• 1️⃣5️⃣ — Бронза 🥉 → 1,5 месяца к абонементу',
    '• 3️⃣5️⃣ — Серебро 🥈 → 4 месяца или бренд-носки',
    '• 5️⃣0️⃣ — Золото 🥇 → 6 месяцев или бренд-полотенце',
    '• 7️⃣0️⃣ — Платина 🪙 → 8 мес + 1 мес заморозки или шейкер & футболка',
    '• 1️⃣0️⃣0️⃣ — Бриллиант 💎 → 12 мес + 1,5 мес заморозки или бренд-пак',
    '• 2️⃣0️⃣0️⃣ — Легенда ⭐️ → Амбассадор бренда',
    '',
    'Детали наград уточняйте у менеджера.',
    'Привилегии Статуса — у Руководителя сети.'
  ].join('\n');
}
bot.hears('Программа лояльности', async (ctx) => {
  const db = loadDB();
  const fid = db.assets['banner_bonuses'];
  if (fid){
    try{ await tgSend.photo(ctx.chat.id, fid, {caption: programText()}); return; }
    catch(_){}
  }
  await tgSend.text(ctx.chat.id, programText(), askChannelOrLink());
});

// [FIX-3]
bot.hears('Получить и обменять Метрики', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'METRICS_MAIN'); await showUI(ctx,'METRICS_MAIN'); });

bot.hears('Проверить баллы и статус', async (ctx)=>{
  const db=loadDB(); const me=ensureMember(db, ctx.from); recalcAndSave(db, me);
  const pts=me.points||0, cur=tierForPoints(pts), nxt=nextTierInfo(pts);
  await sendCard(ctx,'Текущий статус',['Метрики: '+pts,'Статус: '+cur.name],undefined,'banner_bonuses');
  if (nxt) await tgSend.text(ctx.chat.id, box('До следующего статуса', [nxt.name, progressBar(pts, nxt.threshold)]), {parse_mode:'Markdown'});
  else await tgSend.text(ctx.chat.id, 'Ты достиг максимального статуса 🎉');
  const last=db.activity_submissions.filter(a=>a.user_id===me.id&&a.status==='approved').sort((a,b)=>b.ts-a.ts).slice(0,5);
  if (last.length){ const list=last.map(a=>'• '+(ACTIVITIES[a.type]?.title||a.type)+'  +'+(ACTIVITIES[a.type]?.points||0)); await tgSend.text(ctx.chat.id, box('Последние активности', list.join('\n')), {parse_mode:'Markdown'}); }
});

bot.hears('Проверить Метрики за активность', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'ACTIVITY_MENU'); await showUI(ctx,'ACTIVITY_MENU'); });
const labelToActivityKey=(l)=> l==='Отзыв 2ГИС'?'review_2gis': l==='Отзыв Яндекс'?'review_yandex': l==='История ВК'?'story_vk': l==='Статус WA'?'status_wa': l==='История Telegram'?'story_tg': null;
bot.hears(['Отзыв 2ГИС','Отзыв Яндекс','История ВК','Статус WA','История Telegram'], async (ctx)=>{
  const key=labelToActivityKey(ctx.message.text); if(!key) return;
  const db=loadDB(); const m=ensureMember(db, ctx.from); m.ui_prev=m.ui; m.ui='SUBMIT_'+key; saveDB(db);
  await sendCard(ctx,'Активность',[ACTIVITIES[key].title,'Прикрепите скриншот или пришлите ссылку.'],undefined,'banner_activity');
});

// Общий текстовый обработчик
bot.on('text', async (ctx, next)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from); const text=(ctx.message.text||'').trim();

  // Покупка — форма
  if (m.ui==='BUY_FORM'){
    const kb = m.referred_by ? Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить друга',`confirm_ref_${m.id}_${m.referred_by}`), Markup.button.callback('❌ Отклонить',`reject_ref_${m.id}_${m.referred_by}`)]]) : undefined;
    for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `🧾 Заявка на абонемент\n${text}\nОт: @${m.username||'-'} (${m.id})`, kb);}catch(_){ } }
    setUI(db, m.id,'MAIN',{noHistory:true});
    await sendCard(ctx,'Спасибо!','В течение 10 минут менеджер свяжется.', mainMenuFor(ctx),'banner_want');
    return;
  }

  // «Я уже клиент» — по номеру договора
  if (m.ui==='I_AM_CLIENT_CONTRACT'){
    m.contract_no=text; saveDB(db);
    const kb=Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить клиента',`confirm_client_${m.id}`), Markup.button.callback('❌ Отклонить',`reject_client_${m.id}`)]]);
    for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `📂 Запрос «Я уже клиент»\nПо договору: ${text}\nОт: @${m.username||'-'} (${m.id})`, kb);}catch(_){ } }
    await sendCard(ctx,'Спасибо!','В течение 10 минут менеджер подтвердит статус.', mainMenuFor(ctx),'banner_main');
    setUI(db, m.id, 'MAIN', {noHistory:true});
    return;
  }

  // Свободный вопрос
  if (m.ui==='ASK_FREE_TEXT'){
    for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `📝 Вопрос\nОт: @${m.username||'-'} (${m.id})\n${text}`);}catch(_){ } }
    setUI(db, m.id,'MAIN',{noHistory:true});
    await sendCard(ctx,'Спасибо!','Ответим в ближайшее время.', mainMenuFor(ctx),'banner_support');
    return;
  }

  // Подтверждение активностей (ссылкой)
  if (m.ui && m.ui.startsWith('SUBMIT_')){
    const key=m.ui.replace('SUBMIT_','');
    const isLink=/^https?:\/\//i.test(text);
    if (!isLink){ await tgSend.text(ctx.chat.id, 'Пришлите ссылку (или скриншот сообщением).'); return; }
    const id=nextSubmissionId(db);
    db.activity_submissions.push({id,user_id:m.id,type:key,evidence_type:'link',evidence_value:text,status:'pending',approved_by:null,ts:Date.now()}); saveDB(db);
    const title=ACTIVITIES[key]?.title||key;
    const kb=Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить','approve_act_'+id),Markup.button.callback('❌ Отклонить','reject_act_'+id)]]);
    for (const a of ADMIN_IDS){ try{ await tgSend.text(a, `📝 Заявка #${id}\nПользователь: @${m.username||'-'} (${m.id})\nАктивность: ${title}\nДоказательство: ${text}\nБаллы: +${ACTIVITIES[key]?.points||'?'}`, kb);}catch(_){ } }
    await sendCard(ctx,'Заявка отправлена','Спасибо!', metricsMainMenu(),'banner_activity');
    setUI(db, m.id, 'METRICS_MAIN');
    return;
  }

  return next();
});

// Фото подтверждения активности
bot.on('photo', async (ctx, next)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  if (isAdmin(ctx) && db.config.pending_asset_key) return next();
  if (!(m.ui && m.ui.startsWith('SUBMIT_'))) return next();

  const key=m.ui.replace('SUBMIT_','');
  const best=ctx.message.photo.at(-1);
  const id=nextSubmissionId(db);
  db.activity_submissions.push({id,user_id:m.id,type:key,evidence_type:'photo',evidence_value:best.file_id,status:'pending',approved_by:null,ts:Date.now()}); saveDB(db);
  const title=ACTIVITIES[key]?.title||key;
  const kb=Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить','approve_act_'+id),Markup.button.callback('❌ Отклонить','reject_act_'+id)]]);
  for (const a of ADMIN_IDS){
    try{ await tgSend.photo(a, best.file_id, Object.assign({caption:`📝 Заявка #${id}\nПользователь: @${m.username||'-'} (${m.id})\nАктивность: ${title}\nБаллы: +${ACTIVITIES[key]?.points||'?'}`}, kb)); }
    catch{ await tgSend.text(a, `📝 Заявка #${id} (фото)\nПользователь: @${m.username||'-'} (${m.id})\nАктивность: ${title}`, kb); }
  }
  await sendCard(ctx,'Заявка отправлена','Спасибо!', metricsMainMenu(),'banner_activity');
  setUI(db, m.id, 'METRICS_MAIN');
});

// Модерация активностей
bot.action(/^approve_act_/i, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch(_){}
  const id=Number((ctx.callbackQuery?.data||'').split('_')[2]);
  const db=loadDB(); const rec=db.activity_submissions.find(x=>x.id===id);
  if(!rec||rec.status!=='pending') return tgSend.text(ctx.chat.id,'Заявка уже обработана.');
  rec.status='approved'; rec.approved_by=ctx.from.id; saveDB(db);
  const user=db.members[rec.user_id];
  if (user){
    const add=ACTIVITIES[rec.type]?.points||0; user.points=(user.points||0)+add; const before=user.tier||'Нет статуса';
    const after=tierForPoints(user.points); user.tier=after.name; saveDB(db);
    try{ await tgSend.text(user.id, `✅ Подтверждена активность: ${ACTIVITIES[rec.type]?.title||rec.type}\n+${add} метрик(ов)\nТекущие: ${user.points} (${after.name})`);}catch(_){}
    if (after.name!==before && after.name!=='Нет статуса'){ try{ await tgSend.text(user.id, `Повышение статуса: ${after.name} — ${after.reward}`);}catch(_){ } }
  }
  tgSend.text(ctx.chat.id,'Подтверждено ✅');
});
bot.action(/^reject_act_/i, async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch(_){ }
  const id=Number((ctx.callbackQuery?.data||'').split('_')[2]); const db=loadDB(); const rec=db.activity_submissions.find(x=>x.id===id);
  if(!rec||rec.status!=='pending') return tgSend.text(ctx.chat.id,'Заявка уже обработана.');
  rec.status='rejected'; rec.approved_by=ctx.from.id; saveDB(db);
  try{ await tgSend.text(rec.user_id, `❌ Заявка #${id} отклонена. Отправьте новую с корректным подтверждением.`);}catch(_){}
  tgSend.text(ctx.chat.id,'Отклонено.');
});

bot.action('pp_confirm', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const db = loadDB();
  const st = db.pending_purchase[ctx.from.id];
  if (!st) return tgSend.text(ctx.chat.id, 'Нет активной проверки.');

  const refName = st.referrer_id ? '@' + (db.members[st.referrer_id].username || db.members[st.referrer_id].id) : 'пригласитель не найден';
  const kb = Markup.inlineKeyboard([
    [Markup.button.callback('✅ Начислить', 'pp_award'),
     Markup.button.callback('✖ Отменить', 'pp_cancel')]
  ]);
  await tgSend.text(ctx.chat.id, `Начислить 10 метриков ${refName}?`, kb);
});

bot.action('pp_reject', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const db = loadDB();
  const st = db.pending_purchase[ctx.from.id];
  if (!st) return tgSend.text(ctx.chat.id, 'Нет активной проверки.');

  const kb = Markup.inlineKeyboard([
    [Markup.button.callback('📨 Отправить', 'pp_notify_ref'),
     Markup.button.callback('Нет', 'pp_cancel')]
  ]);
  await tgSend.text(ctx.chat.id, 'Отправить пригласителю просьбу узнать причину отказа?', kb);
});

// === ЗАМЕНИТЕ ВЕСЬ ОБРАБОТЧИК НА ЭТОТ ===
bot.action('pp_award', async (ctx)=>{
  try { await ctx.answerCbQuery(); } catch {}
  const db = loadDB();
  const st = db.pending_purchase[ctx.from.id];
  if (!st) return tgSend.text(ctx.chat.id, 'Нет активной проверки.');

  const inviteeId = st.invitee_id;
  const refcode   = st.refcode || (st.referrer_id ? db.members[st.referrer_id].ref_code : null);

  if (inviteeId && refcode) {
    const had = db.referrals.some(r => r.invitee_id === inviteeId && r.stage === 'purchased');

    // 1) фиксируем сам факт покупки (в БД)
    db.referrals.push({
      referrer_code: refcode,
      invitee_id: inviteeId,
      stage: 'purchased',
      phone: st.phone,
      ts: Date.now()
    });
    saveDB(db);

    // 2) обработка пригласителя и уведомление
let ref = st.referrer_id ? db.members[st.referrer_id] : null;
if (!ref) {
  ref = Object.values(db.members).find(u => normCode(u.ref_code) === normCode(refcode)) || null;
}

if (ref) {
  const before = ref.tier || 'Нет статуса';

  if (!had) {
    // очки/статус пересчитываем только при первом подтверждении
    recalcAndSave(db, ref);
  } else {
    // при повторном подтверждении очки не меняем
    saveDB(db);
  }

  // ✅ уведомляем пригласителя ВСЕГДА (как при активностях)
  await notifyReferrerAboutAward(
    db,
    { referrer_id: ref.id, refcode, invitee_id: inviteeId },
    ctx
  );

  // отдельное сообщение о повышении — только при первом подтверждении
  if (!had) {
    const after = tierForPoints(ref.points);
    if (after.name !== before && after.name !== 'Нет статуса') {
      try { await tgSend.text(ref.id, `Повышение статуса: ${after.name} — ${after.reward}`); } catch {}
    }
  }
}

  }

  delete db.pending_purchase[ctx.from.id]; saveDB(db);
  await tgSend.text(ctx.chat.id, 'Начисление выполнено ✅');
});


bot.action('pp_notify_ref', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const db = loadDB();
  const st = db.pending_purchase[ctx.from.id];
  if (!st || !st.referrer_id || !st.invitee_id) {
    return tgSend.text(ctx.chat.id, 'Недостаточно данных.');
  }
  const ref = db.members[st.referrer_id];
  const inv = db.members[st.invitee_id];
  try {
    await tgSend.text(ref.id,
      `Ваш друг @${inv.username || inv.id} по какой-то причине отказался от покупки абонемента. ` +
      `Попросите друга поделиться обратной связью с вами или в чате с ботом. ` +
      `Менеджер начислит дополнительные метрики за активность.`
    );
  } catch {}
  delete db.pending_purchase[ctx.from.id]; saveDB(db);
  await tgSend.text(ctx.chat.id, 'Доставлено 📬');
});

bot.action('pp_cancel', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const db = loadDB();
  delete db.pending_purchase[ctx.from.id];
  saveDB(db);
  await tgSend.text(ctx.chat.id, 'Отменено.');
});

// Обмен Метриков
bot.hears('Обменять Метрики', async (ctx)=>{
  const db=loadDB(); const me=ensureMember(db, ctx.from); recalcAndSave(db, me);
  const pts=me.points||0; const cur=tierForPoints(pts);
  const available=TIERS.filter(t=>pts>=t.threshold).map(t=>'• '+t.name+' — '+t.reward);
  if (!available.length){
    const next=nextTierInfo(pts); if (next) await tgSend.text(ctx.chat.id, `Нужно ещё ${Math.max(0,next.threshold-pts)} до статуса ${next.name} — ${next.reward}`); else await tgSend.text(ctx.chat.id,'Пока недостаточно метриков для обмена.');
    return;
  }
  await tgSend.text(
    ctx.chat.id,
    'Доступные награды (' + cur.name + '):\n' + available.join('\n') + '\n\nНажми кнопку — менеджер оформит вручную.'
  );

  const kb=Markup.inlineKeyboard([[Markup.button.callback('Запросить обмен (статус: '+cur.name+')','redeem_points_'+me.id)]]);
  await tgSend.text(ctx.chat.id,'Оформить обмен:', kb);
});
bot.action(/^redeem_points_/i, async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch(_){ }
  const uid=Number((ctx.callbackQuery?.data||'').split('_')[2]); const db=loadDB(); const m=db.members[uid]; if(!m) return tgSend.text(ctx.chat.id,'Пользователь не найден.');
  const cur=tierForPoints(m.points||0);
  for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `♻️ Запрос на обмен\nID: ${m.id}\nUser: @${m.username||'-'}\nМетрики: ${m.points||0}\nСтатус: ${cur.name}`);}catch(_){ } }
  tgSend.text(ctx.chat.id,'Запрос отправлен менеджеру ✅');
});

// Акции
const promoKeyByTitle=(t)=> t==='Трейд-ин'?'tradein': t==='День рождения'?'birthday': t==='Студент'?'student': null;
bot.hears('Акции для друзей', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'PROMOS_IN_STATUS'); await showUI(ctx,'PROMOS_IN_STATUS'); });
bot.hears(['Трейд-ин','День рождения','Студент'], async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from); if(m.ui!=='PROMOS_IN_STATUS') return;
  const key=promoKeyByTitle(ctx.message.text); if(!key) return; m.tmp_promo=key; saveDB(db);
  const text={ tradein:'Трейд-ин — до 6 месяцев в подарок при покупке абонемента 💪',
               birthday:'День рождения — 7 дней до/после даты 🎁',
               student:'Студент — доп. скидка 📚' }[key];
  await sendCard(ctx,'Акция', text, Markup.keyboard([['Приглашу друга по ссылке'],['Назад']]).resize(),'banner_promos');
});
bot.hears('Приглашу друга по ссылке', sendReferralFast);

// Новости / Расписание
bot.hears('Новости клуба', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'NEWS'); await showUI(ctx,'NEWS'); });
bot.hears('Что нового?', async (ctx)=>{ const db=loadDB(); if(db.news.length){ const last=db.news.at(-1); await sendCard(ctx,'Последнее', last.text, askChannelOrLink(),'banner_news'); } else { await sendCard(ctx,'Новости','Пока нет закреплённых новостей.', askChannelOrLink(),'banner_news'); } });
bot.hears('Расписание групповых тренировок', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'SCHEDULE'); await showUI(ctx,'SCHEDULE'); });
bot.hears(/^(Дропин|Ключевой)$/, async (ctx)=>{ const clubBtn=ctx.match[0]; const key=clubBtn.replace(/\s+/g,''); const db=loadDB(); const assetKey=(clubBtn==='Дропин')?'schedule_dropin':'schedule_klyuchevoy'; if(db.assets[assetKey]) try{ await tgSend.photo(ctx.chat.id, db.assets[assetKey]); }catch(_){ } const txt=db.schedules[key]||(`Расписание для ${clubBtn} ещё не добавлено.`); await tgSend.text(ctx.chat.id, txt); });
bot.hears('Когда ближайшие соревнования', (ctx)=>{ const db=loadDB(); if(db.events.length){ const top=db.events.slice().sort((a,b)=>b.ts-a.ts).slice(0,3).map(e=>'• '+e.text).join('\n'); tgSend.text(ctx.chat.id, top); } else tgSend.text(ctx.chat.id,'Скоро объявим даты — следите за «Что нового?».'); });

// Вопросы / Тренер
bot.hears('Задать вопрос', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'SUPPORT'); await showUI(ctx,'SUPPORT'); });
bot.hears('Менеджер', (ctx)=>{ const db=loadDB(); const m=ensureMember(db, ctx.from); if(m.ui!=='SUPPORT') return; setUI(db, m.id,'SUPPORT_MANAGER'); showUI(ctx,'SUPPORT_MANAGER'); });
bot.hears('Тренер', (ctx)=>{ const db=loadDB(); const m=ensureMember(db, ctx.from); if(m.ui!=='SUPPORT') return; setUI(db, m.id,'SUPPORT_COACH'); showUI(ctx,'SUPPORT_COACH'); });
bot.hears('Составить план питания', async (ctx)=>{ const me=ctx.from; for(const id of ADMIN_IDS){ try{ await tgSend.text(id, `🥗 План питания\nОт: @${me.username||'-'} (${me.id})`);}catch(_){ } } await sendCard(ctx,'Спасибо!','В течение 10 минут тренер свяжется.'); });
bot.hears('Записаться на тренировку', async (ctx)=>{ const me=ctx.from; for(const id of ADMIN_IDS){ try{ await tgSend.text(id, `🏋️ Запись на тренировку\nОт: @${me.username||'-'} (${me.id})`);}catch(_){ } } await sendCard(ctx,'Спасибо!','В течение 10 минут тренер свяжется.'); });

// [NEW] Хочу блок тренировок — меню пакетов и заявка
bot.hears('Хочу блок тренировок', async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  if(m.ui!=='SUPPORT_COACH') setUI(db, m.id, 'SUPPORT_COACH');
  setUI(db, m.id, 'COACH_PACKS');
  await sendCard(ctx,'Выберите пакет', 'Доступные варианты:', coachPackMenu(),'banner_support');
});
bot.hears(['1 Тренировка - 3 500','10 Тренировка - 22 000','15 Тренировка - 30 000','20 Тренировка - 38 000'], async (ctx)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from);
  if (m.ui!=='COACH_PACKS') return;
  const pack = ctx.message.text;
  for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `🏷 Запрос на блок тренировок\nПакет: ${pack}\nОт: @${m.username||'-'} (${m.id})`);}catch(_){ } }
  await sendCard(ctx,'Спасибо!','В течение 10 минут тренер свяжется.', supportCoachMenu(),'banner_support');
  setUI(db, m.id, 'SUPPORT_COACH');
});
bot.hears('Получить консультацию тренера', async (ctx)=>{ const me=ctx.from; for(const id of ADMIN_IDS){ try{ await tgSend.text(id, `🧠 Консультация тренера\nОт: @${me.username||'-'} (${me.id})`);}catch(_){ } } await sendCard(ctx,'Спасибо!','В течение 10 минут тренер свяжется.'); });
bot.hears('Другой вопрос', async (ctx)=>{ const db=loadDB(); const m=ensureMember(db, ctx.from); m.ui_prev=m.ui; m.ui='ASK_FREE_TEXT'; saveDB(db); await sendCard(ctx,'Опишите вопрос','Одним сообщением — передадим менеджеру/тренеру.', Markup.keyboard([['Назад']]).resize(),'banner_support'); });

// Отзывы
bot.hears('Оставить отзыв', async (ctx)=>{ const db=loadDB(); setUI(db, ctx.from.id,'FEEDBACK'); await sendCard(ctx,'Обратная связь','Выберите клуб:', feedbackClubsMenu(),'banner_feedback'); });
bot.hears(['Клуб на Борисовских Прудах, 26 — ТРЦ «Ключевой»','Клуб на Профсоюзной, 118 — ТЦ «Дропин»'], async (ctx,next)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from); if(m.ui!=='FEEDBACK') return next();
  const key=clubKeyByTitleFeedback(ctx.message.text); const urls=FEEDBACK_URLS[key];
  const kb=Markup.inlineKeyboard([[Markup.button.url('Оставить отзыв в 2ГИС', urls.dgis)],[Markup.button.url('Оставить отзыв в Яндекс-Карты', urls.yandex)]]);
  await sendCard(ctx,'Спасибо!','Открой нужную площадку и оставь отзыв 🙏', kb,'banner_feedback');
});

// Контакт (телефон)
bot.on('contact', async (ctx)=>{
  const raw=ctx.message.contact?.phone_number; const phone=normPhone(raw);
  const db=loadDB(); const m=ensureMember(db, ctx.from); m.phone=phone; saveDB(db);

  if (m.referred_by){
    const dupe=db.referrals.find(r=>r.referrer_code===m.referred_by && r.invitee_id===m.id && r.stage==='phone');
    if(!dupe){ db.referrals.push({referrer_code:m.referred_by, invitee_id:m.id, stage:'phone', phone, ts:Date.now()}); saveDB(db); }
  }

  if (m.ui==='I_AM_CLIENT_PHONE_WAIT'){
    const kb=Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить клиента',`confirm_client_${m.id}`), Markup.button.callback('❌ Отклонить',`reject_client_${m.id}`)]]);
    for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `📂 Запрос «Я уже клиент»\nТелефон: ${phone}\nОт: @${m.username||'-'} (${m.id})`, kb);}catch(_){ } }
    await sendCard(ctx,'Спасибо!','В течение 10 минут менеджер подтвердит статус.', mainMenuFor(ctx),'banner_main');
    setUI(db, m.id,'MAIN',{noHistory:true});
    return;
  }

  const kb = m.referred_by ? Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить друга',`confirm_ref_${m.id}_${m.referred_by}`), Markup.button.callback('❌ Отклонить',`reject_ref_${m.id}_${m.referred_by}`)]]) : undefined;
  for (const id of ADMIN_IDS){ try{ await tgSend.text(id, `🆕 Контакт клиента\nИмя: ${m.first_name||''}\nUser: @${m.username||'-'}\nID: ${m.id}\nТелефон: ${phone}\nUI: ${m.ui||'-'}`, kb);}catch(_){ } }
  await sendCard(ctx,'Спасибо!','Номер сохранён. Менеджер свяжется 📞', mainMenuFor(ctx),'banner_support');
  setUI(db, m.id,'MAIN',{noHistory:true});
});

// Меню менеджера
bot.hears('Меню менеджера', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); setUI(db, ctx.from.id,'ADM_MENU'); await sendCard(ctx,'Меню менеджера','Выберите действие:', adminMenu(),'banner_main'); });
bot.hears('Заявки на активности', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); const pending=db.activity_submissions.filter(a=>a.status==='pending').sort((a,b)=>a.id-b.id); if(!pending.length) return tgSend.text(ctx.chat.id,'Нет заявок в ожидании ✅'); await tgSend.text(ctx.chat.id,`Заявок в ожидании: ${pending.length}`); for (const rec of pending.slice(0,15)){ const user=db.members[rec.user_id]||{}; const title=ACTIVITIES[rec.type]?.title||rec.type; const kb=Markup.inlineKeyboard([[Markup.button.callback('✅ Подтвердить','approve_act_'+rec.id),Markup.button.callback('❌ Отклонить','reject_act_'+rec.id)]]); const caption=`#${rec.id} • ${title}\nПользователь: @${user.username||'-'} (${rec.user_id})\nДоказательство: ${rec.evidence_type==='link'?rec.evidence_value:rec.evidence_type}`; if(rec.evidence_type==='photo'){ try{ await tgSend.photo(ctx.chat.id, rec.evidence_value, Object.assign({caption:caption}, kb)); }catch(_){ await tgSend.text(ctx.chat.id, caption, kb);} } else { await tgSend.text(ctx.chat.id, caption, kb);} } });
bot.hears('Отметить покупку', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); setUI(db, ctx.from.id,'ADM_MARK_PURCHASE'); await tgSend.text(ctx.chat.id,'Пришлите номер формата +7XXXXXXXXXX'); });

bot.on('text', async (ctx, next)=>{
  const db = loadDB();
  const m  = ensureMember(db, ctx.from);
  const t  = (ctx.message.text || '').trim();
  const isPhone = /^\+?\d[\d\-\s\(\)]{6,}$/.test(t);

  // старт сценария проверки покупки по номеру
  if (isAdmin(ctx) && m.ui === 'ADM_MARK_PURCHASE' && isPhone) {
    const phone = normPhone(t);

    const invitee = Object.values(db.members).find(u => u.phone === phone) || null;

    const lastRef = invitee
      ? [...db.referrals].reverse().find(r =>
          (r.invitee_id === invitee.id || r.phone === phone) &&
          (r.stage === 'started' || r.stage === 'phone'))
      : null;

    const referrer = lastRef
      ? Object.values(db.members).find(u => normCode(u.ref_code) === normCode(lastRef.referrer_code))
      : null;

    db.pending_purchase[ctx.from.id] = {
      phone,
      invitee_id: invitee?.id || null,
      referrer_id: referrer?.id || null,
      refcode: lastRef?.referrer_code || null
    };
    saveDB(db);

    const refName = referrer ? ('@' + (referrer.username || referrer.id)) : 'не найден';
    const kb = Markup.inlineKeyboard([
      [Markup.button.callback('✅ Подтвердить', 'pp_confirm'),
       Markup.button.callback('❌ Отказ', 'pp_reject')]
    ]);

    await tgSend.text(
      ctx.chat.id,
      `Найден клиент с номером ${phone}\nПригласитель: ${refName}\nКлиент приобрёл абонемент?`,
      kb
    );
    return;
  }

  return next();
});

bot.command('mark_phone', async (ctx)=>{
  if(!isAdmin(ctx)) return;
  const p=ctx.message.text.trim().split(/\s+/);
  if(p.length<3) return tgSend.text(ctx.chat.id,'Исп: /mark_phone <+7...> <purchased>');
  const phone=normPhone(p[1]); const stage=p[2];
  const db=loadDB(); const mem=Object.values(db.members).find(m=>m.phone===phone);
  if(!mem) return tgSend.text(ctx.chat.id,'Клиент с таким телефоном не найден.');
  const last=[].concat(db.referrals).reverse().find(r=>r.invitee_id===mem.id || r.phone===phone);
  if(!last) return tgSend.text(ctx.chat.id,'Для этого клиента нет данных о реферале.');
  const hadPurchased=db.referrals.some(r=>(r.invitee_id===mem.id||r.phone===phone)&&r.stage==='purchased');
  db.referrals.push({referrer_code:last.referrer_code, invitee_id:mem.id, stage, phone, ts:Date.now()}); saveDB(db);
  if (stage==='purchased' && !hadPurchased){
    const ref = Object.values(db.members).find(m => normCode(m.ref_code) === normCode(last.referrer_code)) || null;
    if (ref){
      const before = ref.tier || 'Нет статуса';
      recalcAndSave(db, ref);

      await notifyReferrerAboutAward(db, { referrer_id: ref.id, refcode: last.referrer_code, invitee_id: mem.id }, ctx);

      const after = tierForPoints(ref.points);
      if (after.name !== before && after.name !== 'Нет статуса') {
        try { await tgSend.text(ref.id, `Повышение статуса: ${after.name} — ${after.reward}`);} catch(_){}
      }
    }
  }
  tgSend.text(ctx.chat.id,'Отмечено по телефону ✅');
});

// подтверждение друга (кнопки приходят на заявку/контакт)
bot.action(/^confirm_ref_/i, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch(_){}

  const parts=(ctx.callbackQuery?.data||'').split('_'); // confirm_ref_<inviteeId>_<refcode>
  const inviteeId=Number(parts[2]);
  const refcode=parts.slice(3).join('_');

  const db=loadDB();

  const had=db.referrals.some(r=>r.invitee_id===inviteeId && r.stage==='purchased');
  db.referrals.push({referrer_code:refcode, invitee_id:inviteeId, stage:'purchased', phone:null, ts:Date.now()});
  saveDB(db);

    // --- обработка пригласителя и уведомление ---
  // найдём пригласителя по коду
  const ref = Object.values(db.members).find(
    u => normCode(u.ref_code) === normCode(refcode)
  ) || null;

  const before = ref ? (ref.tier || 'Нет статуса') : 'Нет статуса';

  if (!had && ref) {
    // очки/статус пересчитываем ТОЛЬКО при первом подтверждении
    recalcAndSave(db, ref);
  } else {
    // при повторном подтверждении очки не меняем — просто сохраняем БД
    saveDB(db);
  }

  // уведомляем пригласителя ВСЕГДА (как при активностях)
  await notifyReferrerAboutAward(
    db,
    { referrer_id: ref ? ref.id : null, refcode, invitee_id: inviteeId },
    ctx
  );

  // отдельное сообщение о повышении — только при первом подтверждении
  if (!had && ref) {
    const after = tierForPoints(ref.points);
    if (after.name !== before && after.name !== 'Нет статуса') {
      try { await tgSend.text(ref.id, `Повышение статуса: ${after.name} — ${after.reward}`); } catch {}
    }
  }


  tgSend.text(ctx.chat.id,'Подтверждено ✅');
});
bot.action(/^reject_ref_/i, async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch(_){ } tgSend.text(ctx.chat.id,'Отклонено.'); });

// подтверждение/отклонение «я уже клиент»
bot.action(/^confirm_client_/i, async (ctx)=>{
  try { await ctx.answerCbQuery(); } catch(_) {}
  const uid = Number((ctx.callbackQuery?.data || '').split('_')[2]);
  const db  = loadDB();
  const m   = db.members[uid];
  if (!m) return tgSend.text(ctx.chat.id, 'Не найден.');

  m.is_client = true;
  saveDB(db);

  try { await tgSend.text(uid, 'Ваш статус клиента подтверждён ✅', mainMenuFor({ from: { id: uid } })); } catch(_){}
  tgSend.text(ctx.chat.id, 'Подтверждено ✅');
});
bot.action(/^reject_client_/i, async (ctx)=>{
  try { await ctx.answerCbQuery(); } catch(_) {}
  const uid = Number((ctx.callbackQuery?.data || '').split('_')[2]);
  const db  = loadDB();
  const m   = db.members[uid];
  if (!m) return tgSend.text(ctx.chat.id, 'Не найден.');

  m.is_client = false;
  saveDB(db);

  try { await tgSend.text(uid, '❌ Запрос на подтверждение статуса отклонён. Свяжитесь с менеджером для уточнения.'); } catch(_){}
  tgSend.text(ctx.chat.id, 'Отклонено.');
});

// Добавить расписание
bot.hears('Добавить расписание', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); setUI(db, ctx.from.id,'ADM_SCHED_SELECT'); await sendCard(ctx,'Выберите клуб','', adminAddScheduleMenu(),'banner_main'); });
bot.hears(['Клуб на Профсоюзной, 118 — ТЦ «Дропин»','Клуб на Борисовских Прудах, 26 — ТРЦ «Ключевой»'], async (ctx,next)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from); if(!isAdmin(ctx)||m.ui!=='ADM_SCHED_SELECT') return next();
  const club=ctx.message.text.includes('Дропин')?'schedule_dropin':'schedule_klyuchevoy';
  db.config.pending_asset_key=club; saveDB(db);
  setUI(db, m.id, 'ADM_SCHED_UPLOAD');
  await tgSend.text(ctx.chat.id,'Отправьте фото — старое будет заменено.');
});
bot.on('photo', async (ctx,next)=>{ const db=loadDB(); const m=ensureMember(db, ctx.from); if(isAdmin(ctx)&&m.ui==='ADM_SCHED_UPLOAD'){ const best=ctx.message.photo.at(-1); const key=db.config.pending_asset_key; if(key){ db.assets[key]=best.file_id; db.config.pending_asset_key=null; saveDB(db);} setUI(db, m.id, 'ADM_MENU'); return; } return next(); });

// Рассылка с предпросмотром
bot.hears('Рассылка всем', async (ctx)=>{ if(!isAdmin(ctx)) return; const db=loadDB(); const m=ensureMember(db, ctx.from); m.bcast_draft={text:null}; saveDB(db); setUI(db, m.id,'ADM_BCAST_EDIT'); tgSend.text(ctx.chat.id,'Введите текст рассылки.'); });
bot.on('text', async (ctx,next)=>{
  const db=loadDB(); const m=ensureMember(db, ctx.from); const t=(ctx.message.text||'').trim();
  if (isAdmin(ctx)&&m.ui==='ADM_BCAST_EDIT'){
    m.bcast_draft.text=t; saveDB(db); setUI(db, m.id,'ADM_BCAST_PREVIEW');
    const kb=Markup.inlineKeyboard([[Markup.button.callback('🚀 Отправить всем','bcast_send')],[Markup.button.callback('✍️ Изменить','bcast_edit')]]);
    await tgSend.text(ctx.chat.id, `Предпросмотр:\n${t}`, kb); return;
  }
  return next();
});
bot.action('bcast_edit', async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch(_){ } const db=loadDB(); const m=ensureMember(db, ctx.from); setUI(db, m.id,'ADM_BCAST_EDIT'); tgSend.text(ctx.chat.id,'Пришлите новый текст.'); });
bot.action('bcast_send', async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch(_){ } const db=loadDB(); const m=ensureMember(db, ctx.from); const t=m.bcast_draft?.text||''; if(!t) return tgSend.text(ctx.chat.id,'Пусто.'); let ok=0,fail=0; for(const u of Object.values(db.members)){ try{ await tgSend.text(u.id,t); ok++; }catch{ fail++; } } setUI(db, m.id,'ADM_MENU'); tgSend.text(ctx.chat.id,`Готово. Успешно: ${ok}, ошибок: ${fail}`); });

// Reminders
function runReminders(){
  const db=loadDB(); const now=Date.now(); if(now-(db.config.last_reminders_run||0)<60*60*1000) return; db.config.last_reminders_run=now; saveDB(db);
  const days=(ms)=>Math.floor(ms/86400000);
  for (const m of Object.values(db.members)){
    if(!m.ref_last_shared_ts) continue; const d=days(now-m.ref_last_shared_ts);
    if(d===7||d===30){ const had=db.referrals.some(r=>r.referrer_code===m.ref_code && r.stage==='started' && r.ts>m.ref_last_shared_ts); if(!had){ tgSend.text(m.id, `Напоминание: поделитесь ссылкой ещё раз — друг пока не перешёл.\n${buildRefLink(m.ref_code)}`); } }
  }
}
setInterval(runReminders, 15*60*1000);

bot.launch().then(()=>console.log('Bot started')).catch(console.error);
process.once('SIGINT', ()=>bot.stop('SIGINT'));
process.once('SIGTERM', ()=>bot.stop('SIGTERM'));
