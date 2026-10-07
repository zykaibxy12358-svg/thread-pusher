/**
 * 多线推进 ThreadPusher —— TauriTavern / SillyTavern 扩展（独立版）
 *
 * 解决的问题：剧情有多个分支/悬念时，用户不主动推进，故事就会卡住不动。
 * 本扩展维护一份「线索台账」：抽取悬挂线索 → 检测停滞 → 用户被动时注入"本轮请推进某条线"的指令。
 *
 * 特点：
 *  · 完全独立：不需要世界书、不需要记忆链、不需要任何外部数据
 *  · 默认零成本：线索抽取用规则（承诺/疑问/消息类句式），不调用模型
 *  · 可选精修：每隔 N 轮用便宜模型抽取一次线索（可关）
 *  · 按"轮"计停滞（不是章），换聊天各自独立
 *
 * 性能：全内存索引、每轮 0 次 I/O；抽取只在窗口内的句子做正则扫描。
 */

(function () {
    'use strict';

    const MODULE = 'threadPusher';
    const NS = 'thread-pusher';
    const DB_NAME = 'threadPusher';
    const DB_VER = 1;
    const STORE_KV = 'kv';
    const STORE_CHAT = 'chat';

    const DEFAULT_SETTINGS = {
        enabled: true,
        intensity: 1,            // 0=关 1=用户被动时 2=每 N 轮必推 3=每轮都推
        staleTurns: 4,           // 多少轮没被碰到算停滞
        mainCount: 1,            // 每轮主推几条
        bgCount: 1,              // 背景信号几条
        cooldownTurns: 3,        // 被推过的线索冷却轮数
        everyTurns: 4,           // 强度 2 的间隔
        depth: 1,                // 注入深度
        scanTurns: 6,            // 每次抽取扫描最近多少轮
        minSentence: 8,          // 线索句最短字数
        passiveChars: 12,        // 用户消息短于此视为被动
        passiveWords: '继续|接着|然后呢|随便|你来|你说|时间|跳过|快进|总结|嗯|哦|好|在吗',
        llmExtract: false,       // 是否用模型精修线索
        llmEveryTurns: 8,        // 模型抽取间隔（轮）
        autoCommit: true,        // 生成后自动记推进/冷却
        debug: false,
    };

    // 句子里的"悬挂信号"
    const RE_PROMISE = /(下次|改天|回头|稍后|晚些|明天|后天|以后再|再说|再谈|等我|等到|要找|得找|去问|查一查|查查|打听|调查|打算|准备|计划|尚未|还没|未曾|不见了|失踪|下落不明|传来|来信|口信|听说|传闻|惦记|挂心|犹豫|等着)/;
    const RE_MSG = /(信|口信|消息|请帖|文书|密报|传言|传闻)/;

    // ---------------------------------------------------------------- 上下文
    let ctx = {};
    let eventSource = null;
    let event_types = {};
    let extTypes = { IN_PROMPT: 0, IN_CHAT: 1, BEFORE_PROMPT: 2 };
    let extRoles = { SYSTEM: 0, USER: 1, ASSISTANT: 2 };

    function bindContext() {
        if (typeof SillyTavern === 'undefined' || typeof SillyTavern.getContext !== 'function') return false;
        let c = null;
        try { c = SillyTavern.getContext(); } catch (e) { return false; }
        if (!c) return false;
        ctx = c;
        if (c.eventSource) eventSource = c.eventSource;
        if (c.event_types) event_types = c.event_types;
        if (c.extension_prompt_types) extTypes = c.extension_prompt_types;
        if (c.extension_prompt_roles) extRoles = c.extension_prompt_roles;
        return true;
    }
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    async function waitForContext(timeoutMs) {
        const limit = Date.now() + (timeoutMs || 10000);
        while (Date.now() < limit) { if (bindContext()) return true; await sleep(200); }
        return bindContext();
    }

    // ---------------------------------------------------------------- 状态
    const S = {
        settings: Object.assign({}, DEFAULT_SETTINGS),
        ns: 'default',
        threads: [],
        state: { lastPushTurn: null, pending: [], lastExtractTurn: 0 },
        stats: { lastMs: 0, planned: 0, injected: 0, at: '' },
        storeNote: '',
    };

    const log = (...a) => { if (S.settings.debug) console.log('[多线推进]', ...a); };
    const warn = (...a) => console.warn('[多线推进]', ...a);
    const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
    const nowMs = () => (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now());

    function nsKey() {
        const ch = (ctx.characters && ctx.characters[ctx.characterId]) || null;
        const name = ch ? ch.name : (ctx.name2 || ctx.groupId || 'unknown');
        const meta = ctx.chatMetadata || {};
        const id = ctx.chatId || meta.chat_id || meta.file_name || 'default';
        return String(name) + '::' + String(id);
    }
    function slugify(ns) {
        let h = 5381;
        for (let i = 0; i < ns.length; i++) h = ((h * 33) ^ ns.charCodeAt(i)) >>> 0;
        const ascii = ns.replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 24);
        return (ascii || 'chat') + '-' + h.toString(36);
    }

    // ---------------------------------------------------------------- 存储
    const Store = {
        kind: 'memory', tauri: null, db: null,
        mem: { kv: new Map(), chat: new Map() }, note: '',

        async detectTauri(timeoutMs) {
            const w = typeof window !== 'undefined' ? window : {};
            const direct = w.__TAURITAVERN__ && w.__TAURITAVERN__.api && w.__TAURITAVERN__.api.extension && w.__TAURITAVERN__.api.extension.store;
            if (direct) return direct;
            const p = (w.__TAURITAVERN__ && w.__TAURITAVERN__.ready) || w.__TAURITAVERN_MAIN_READY__;
            if (!p || typeof p.then !== 'function') return null;
            try { await Promise.race([Promise.resolve(p).catch(() => null), new Promise((r) => setTimeout(r, timeoutMs || 2500))]); } catch (e) { return null; }
            const w2 = typeof window !== 'undefined' ? window : {};
            return (w2.__TAURITAVERN__ && w2.__TAURITAVERN__.api && w2.__TAURITAVERN__.api.extension && w2.__TAURITAVERN__.api.extension.store) || null;
        },

        async init() {
            try {
                const t = await this.detectTauri(2500);
                if (t && typeof t.setJson === 'function') {
                    const probe = 'probe.' + Date.now();
                    await t.setJson({ namespace: NS, key: probe, value: 1 });
                    const back = await t.getJson({ namespace: NS, key: probe });
                    await t.deleteJson({ namespace: NS, key: probe }).catch(() => {});
                    if (back === 1) { this.tauri = t; this.kind = 'tauri'; this.note = 'TauriTavern 原生存储'; return; }
                }
            } catch (e) { warn('TauriTavern store 不可用', e); }

            const db = await this.openIDB();
            if (db) { this.db = db; this.kind = 'idb'; this.note = 'IndexedDB'; return; }
            this.kind = 'memory';
            this.note = this.note || '仅内存（本次会话有效）';
        },

        openIDB() {
            return new Promise((resolve) => {
                if (typeof indexedDB === 'undefined' || !indexedDB) { this.note = '环境无 IndexedDB'; resolve(null); return; }
                let req;
                try { req = indexedDB.open(DB_NAME, DB_VER); } catch (e) { this.note = String(e && e.message); resolve(null); return; }
                req.onupgradeneeded = () => {
                    const db = req.result;
                    if (!db.objectStoreNames.contains(STORE_KV)) db.createObjectStore(STORE_KV);
                    if (!db.objectStoreNames.contains(STORE_CHAT)) db.createObjectStore(STORE_CHAT);
                };
                req.onsuccess = () => resolve(req.result);
                req.onerror = () => { this.note = 'IndexedDB 打开失败'; resolve(null); };
            });
        },

        async saveKV(key, value) {
            try {
                if (this.kind === 'tauri') { await this.tauri.setJson({ namespace: NS, table: 'main', key: key, value: value }); return; }
                if (this.kind === 'idb') { this.db.transaction(STORE_KV, 'readwrite').objectStore(STORE_KV).put(value, key); return; }
                this.mem.kv.set(key, value);
            } catch (e) { warn('保存设置失败', e); }
        },

        async loadKV(key) {
            try {
                if (this.kind === 'tauri') { const r = await this.tauri.tryGetJson({ namespace: NS, table: 'main', key: key }); return (r && r.found) ? r.value : null; }
                if (this.kind === 'idb') {
                    return await new Promise((resolve) => {
                        const req = this.db.transaction(STORE_KV, 'readonly').objectStore(STORE_KV).get(key);
                        req.onsuccess = () => resolve(req.result === undefined ? null : req.result);
                        req.onerror = () => resolve(null);
                    });
                }
                return this.mem.kv.has(key) ? this.mem.kv.get(key) : null;
            } catch (e) { return null; }
        },

        async saveChat(slug, value) {
            try {
                if (this.kind === 'tauri') { await this.tauri.setJson({ namespace: NS, table: 'chats', key: slug, value: value }); return; }
                if (this.kind === 'idb') { this.db.transaction(STORE_CHAT, 'readwrite').objectStore(STORE_CHAT).put(value, slug); return; }
                this.mem.chat.set(slug, value);
            } catch (e) { warn('保存线索失败', e); }
        },

        async loadChat(slug) {
            try {
                if (this.kind === 'tauri') { const r = await this.tauri.tryGetJson({ namespace: NS, table: 'chats', key: slug }); return (r && r.found) ? r.value : null; }
                if (this.kind === 'idb') {
                    return await new Promise((resolve) => {
                        const req = this.db.transaction(STORE_CHAT, 'readonly').objectStore(STORE_CHAT).get(slug);
                        req.onsuccess = () => resolve(req.result === undefined ? null : req.result);
                        req.onerror = () => resolve(null);
                    });
                }
                return this.mem.chat.has(slug) ? this.mem.chat.get(slug) : null;
            } catch (e) { return null; }
        },
    };

    const persistChat = () => { Store.saveChat(slugify(S.ns), { ns: S.ns, threads: S.threads, state: S.state, at: Date.now() }); };

    // ---------------------------------------------------------------- 轮次与文本
    function turnNow() {
        const chat = ctx.chat || [];
        let n = 0;
        for (const m of chat) if (m && m.is_user) n++;
        return n;
    }

    function windowText(turns) {
        const chat = ctx.chat || [];
        const n = clamp(Number(turns) || 6, 1, 40) * 2;
        const parts = [];
        for (let i = Math.max(0, chat.length - n); i < chat.length; i++) {
            const m = chat[i];
            if (!m || m.is_system) continue;
            parts.push(String(m.mes || ''));
        }
        return parts.join('\n');
    }

    function bigrams(s) {
        const out = new Set();
        const t = String(s || '').replace(/[\s\p{P}\p{S}]/gu, '');
        for (let i = 0; i < t.length - 1; i++) out.add(t.slice(i, i + 2));
        return out;
    }
    function overlap(a, b) {
        if (!a || !b || !a.size || !b.size) return 0;
        let n = 0;
        a.forEach((g) => { if (b.has(g)) n++; });
        return n / Math.min(a.size, b.size);
    }

    // ---------------------------------------------------------------- 线索台账
    function ensure() { if (!Array.isArray(S.threads)) S.threads = []; return S.threads; }

    function addThread(spec) {
        const list = ensure();
        const grams = bigrams(spec.title);
        // 与已有线索高度重合 → 视为同一条，只刷新"最近被碰到"的轮次
        let best = null, bestScore = 0;
        for (const t of list) {
            if (t.done) continue;
            const s = overlap(grams, t.grams || bigrams(t.title));
            if (s > bestScore) { bestScore = s; best = t; }
        }
        if (best && bestScore >= 0.5) {
            best.lastSeenTurn = Math.max(best.lastSeenTurn || 0, spec.lastSeenTurn || turnNow());
            if (spec.hint && !best.hint) best.hint = spec.hint;
            if (spec.who && spec.who.length && !(best.who || []).length) best.who = spec.who;
            return best;
        }
        const item = {
            id: 't' + Date.now().toString(36) + Math.floor(Math.random() * 1000).toString(36),
            title: String(spec.title || '').slice(0, 60), source: spec.source || 'rule',
            who: spec.who || [], hint: spec.hint || '',
            grams: grams, lastSeenTurn: spec.lastSeenTurn || turnNow(),
            pushed: 0, cooldownUntilTurn: 0, done: false, createdTurn: turnNow(),
        };
        list.push(item);
        return item;
    }

    /** 规则抽取：从最近的句子里找"悬挂信号" */
    function extractByRules(text) {
        const minLen = Number(S.settings.minSentence) || 8;
        const found = [];
        const sentences = String(text || '')
            .split(/[\n。！!？?；;]+/)
            .map((s) => s.trim())
            .filter((s) => s.length >= minLen && s.length <= 80);
        for (const s of sentences) {
            const isQ = /[？?]/.test(s) || /(为什么|怎么办|如何|是否|难道|是不是)/.test(s);
            const isP = RE_PROMISE.test(s);
            const isM = RE_MSG.test(s) && /(未|没|没有|尚未|迟迟|等|来)/.test(s);
            if (!(isP || isM || isQ)) continue;
            found.push({ title: s.replace(/\s+/g, ' ').slice(0, 48), hint: '', source: 'rule' });
            if (found.length >= 12) break;
        }
        return found;
    }

    /** 模型抽取（可选）：让便宜模型把"还挂着的线"整理成 JSON */
    function buildExtractPrompt() {
        return [
            '你是剧情线索整理器。阅读下面的最近对话，找出"尚未解决、还挂在空中"的线索：',
            '未兑现的承诺、未解开的疑问、未完成的计划、失踪或去向不明的人、待回的信与待办的事。',
            '只输出 JSON 数组，不要解释。每项格式：',
            '{"title":"不超过24字的线索","who":["相关人名"],"hint":"一句话建议怎么推进","done":false}',
            '若某条线索在对话里已经被解决，请以 "done":true 列出，或干脆不列。最多 8 条。',
            '',
            '---最近对话---',
            windowText(S.settings.scanTurns),
            '---结束---',
            'JSON：',
        ].join('\n');
    }

    function parseJSONArray(text) {
        if (!text) return null;
        let t = String(text).trim();
        const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/i);
        if (fence) t = fence[1].trim();
        const s = t.indexOf('['), e = t.lastIndexOf(']');
        if (s >= 0 && e > s) { try { return JSON.parse(t.slice(s, e + 1)); } catch (err) { /* fallthrough */ } }
        const s2 = t.indexOf('{'), e2 = t.lastIndexOf('}');
        if (s2 >= 0 && e2 > s2) { try { const o = JSON.parse(t.slice(s2, e2 + 1)); return Array.isArray(o) ? o : [o]; } catch (err) { /* ignore */ } }
        return null;
    }

    async function extractByModel() {
        if (typeof ctx.generateQuietPrompt !== 'function') return null;
        try {
            const res = await ctx.generateQuietPrompt(buildExtractPrompt(), false, true);
            const arr = parseJSONArray(res);
            if (!arr) { warn('模型抽取解析失败', res); return null; }
            return arr;
        } catch (e) { warn('模型抽取失败', e); return null; }
    }

    /** 扫描并更新台账 */
    async function scan(useModel) {
        const t0 = nowMs();
        const text = windowText(S.settings.scanTurns);
        if (!text) return { added: 0, done: 0 };
        const turn = turnNow();
        const cands = extractByRules(text);
        let added = 0;
        for (const c of cands) { const t = addThread(c); if (t && t.createdTurn === turn) added++; }

        if (useModel && S.settings.llmExtract) {
            const arr = await extractByModel();
            if (Array.isArray(arr)) {
                for (const it of arr) {
                    if (!it || !it.title) continue;
                    if (it.done) {
                        const g = bigrams(it.title);
                        ensure().forEach((t) => { if (!t.done && overlap(g, t.grams || bigrams(t.title)) >= 0.5) t.done = true; });
                        continue;
                    }
                    addThread({ title: it.title, who: it.who || [], hint: it.hint || '', source: 'llm' });
                }
                S.state.lastExtractTurn = turn;
            }
        }
        S.state.pending = S.state.pending.filter((k) => ensure().some((t) => t.id === k && !t.done));
        persistChat();
        S.stats.lastMs = Math.round((nowMs() - t0) * 100) / 100;
        renderThreads();
        log('扫描完成：现有线索', ensure().length);
        return { added: added, total: ensure().length };
    }

    // ---------------------------------------------------------------- 判定与注入
    function isPassive(text, hotIds) {
        const t = String(text || '').trim();
        if (!t) return true;
        if (t.length <= Number(S.settings.passiveChars || 12)) return true;
        const words = String(S.settings.passiveWords || '').split(/[|,，\s]+/).filter(Boolean);
        for (const w of words) if (w && t.indexOf(w) === 0) return true;
        return !hotIds || hotIds.length === 0;      // 没碰到任何线索 → 视为被动
    }

    function hotThreads(text) {
        const g = bigrams(text);
        const hot = [];
        ensure().forEach((t) => {
            if (t.done) return;
            const s = overlap(g, t.grams || bigrams(t.title));
            if (s >= 0.25) { hot.push(t.id); t.lastSeenTurn = Math.max(t.lastSeenTurn || 0, turnNow()); }
        });
        return hot;
    }

    function candidates() {
        const turn = turnNow();
        return ensure()
            .filter((t) => !t.done && turn >= (t.cooldownUntilTurn || 0))
            .map((t) => {
                const stale = t.lastSeenTurn ? (turn - t.lastSeenTurn) : 999;
                return { t: t, stale: stale, score: stale * 0.7 - (t.pushed || 0) * 0.15 };
            })
            .sort((a, b) => b.score - a.score);
    }

    function shouldPush(userText, hotIds) {
        if (!S.settings.enabled) return false;
        const intensity = Number(S.settings.intensity) || 0;
        if (intensity <= 0) return false;
        const turn = turnNow();
        const since = turn - (S.state.lastPushTurn == null ? -999 : S.state.lastPushTurn);
        if (intensity === 3) return true;
        if (intensity === 2) return since >= (Number(S.settings.everyTurns) || 4);
        return isPassive(userText, hotIds) && since >= 1;
    }

    function buildBlock(userText, hotIds) {
        const cands = candidates();
        if (!cands.length) return { block: '', picked: [] };
        const staleMin = Number(S.settings.staleTurns) || 4;
        const mainN = clamp(Number(S.settings.mainCount) || 1, 0, 3);
        const bgN = clamp(Number(S.settings.bgCount) || 1, 0, 3);
        const pool = cands.filter((c) => Number(S.settings.intensity) === 3 || hotIds.indexOf(c.t.id) < 0);
        const use = pool.length ? pool : cands;
        const stalePool = use.filter((c) => c.stale >= staleMin);
        const chosen = (stalePool.length ? stalePool : use).slice(0, mainN + bgN);
        if (!chosen.length) return { block: '', picked: [] };

        const main = chosen.slice(0, mainN);
        const bg = chosen.slice(mainN, mainN + bgN);
        const lines = ['【多线推进·本轮任务】', '玩家这一轮没有主动推动任何线索。请在本轮回复里让故事自己往前走：'];
        main.forEach((c, i) => {
            lines.push((i + 1) + '. 主线推进：' + c.t.title +
                '（已停滞 ' + (c.stale > 90 ? '从未推进' : c.stale + ' 轮') +
                ((c.t.who || []).length ? '｜关联：' + c.t.who.join('、') : '') + '）' +
                (c.t.hint ? '　建议方式：' + c.t.hint : ''));
        });
        if (bg.length) lines.push('背景信号（各一句带过，不要展开）：' + bg.map((c) => c.t.title).join('；'));
        lines.push('规矩：① 本轮只推进上述线索，不要顺手把别的线也解决；② 用场景、对话或消息自然带出，不要写成旁白交代；③ 不替玩家做决定，把选择权留在玩家手里；④ 推进后留下新的小钩子。');
        return { block: lines.join('\n'), picked: chosen.map((c) => c.t) };
    }

    function inject(text) {
        const fn = (ctx && typeof ctx.setExtensionPrompt === 'function') ? ctx.setExtensionPrompt
            : (typeof setExtensionPrompt === 'function' ? setExtensionPrompt : null);
        if (!fn) return;
        fn(MODULE, text || '', extTypes.IN_CHAT, clamp(Number(S.settings.depth) || 1, 0, 100), false, extRoles.SYSTEM);
    }

    function planAndInject(userText) {
        const hotIds = hotThreads(userText || '');
        if (!shouldPush(userText || '', hotIds)) { inject(''); S.state.pending = []; renderThreads(); return null; }
        const r = buildBlock(userText || '', hotIds);
        if (!r.block) { inject(''); S.state.pending = []; renderThreads(); return null; }   // 没有可推进的线索
        inject(r.block);
        S.state.pending = r.picked.map((t) => t.id);
        renderThreads();
        return r;
    }

    function commit() {
        const keys = S.state.pending || [];
        if (!keys.length) return;
        const turn = turnNow();
        const cd = Number(S.settings.cooldownTurns) || 3;
        keys.forEach((id, i) => {
            const t = ensure().find((x) => x.id === id);
            if (!t) return;
            t.pushed = (t.pushed || 0) + 1;
            t.lastSeenTurn = turn;
            t.cooldownUntilTurn = turn + (i === 0 ? 0 : cd);
        });
        S.state.lastPushTurn = turn;
        S.state.pending = [];
        persistChat();
        renderThreads();
    }

    // ---------------------------------------------------------------- UI
    let $panel = null;

    function buildUI() {
        if (typeof $ !== 'function') return;
        const html = `
<div class="thread-pusher-settings">
  <div class="inline-drawer">
    <div class="inline-drawer-toggle inline-drawer-header">
      <b>多线推进 ThreadPusher</b>
      <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
    </div>
    <div class="inline-drawer-content">
      <div id="tp-status" class="tp-status"></div>
      <label class="checkbox_label"><input id="tp-enabled" type="checkbox"><span>启用（用户不推时自动推一条线）</span></label>
      <div class="tp-row"><span>强度（0关 / 1被动时 / 2每N轮 / 3每轮）</span><input id="tp-intensity" type="number" min="0" max="3"></div>
      <div class="tp-row"><span>停滞多少轮算"该推了"</span><input id="tp-stale" type="number" min="1" max="100"></div>
      <div class="tp-row"><span>每轮主线 / 背景信号</span><input id="tp-main" type="number" min="0" max="3"><input id="tp-bg" type="number" min="0" max="3"></div>
      <div class="tp-row"><span>冷却（轮）/ 强度2间隔</span><input id="tp-cd" type="number" min="0" max="50"><input id="tp-every" type="number" min="1" max="50"></div>
      <div class="tp-row"><span>抽取窗口（轮）/ 注入深度</span><input id="tp-scan" type="number" min="1" max="40"><input id="tp-depth" type="number" min="0" max="50"></div>
      <label class="checkbox_label"><input id="tp-llm" type="checkbox"><span>用模型精修线索（每 N 轮一次，可关）</span></label>
      <div class="tp-row"><span>模型抽取间隔（轮）</span><input id="tp-llm-every" type="number" min="2" max="100"></div>
      <label class="checkbox_label"><input id="tp-debug" type="checkbox"><span>调试日志</span></label>

      <div id="tp-list" class="tp-list"></div>

      <div class="tp-buttons">
        <button id="tp-scan" class="menu_button">立即扫描线索</button>
        <button id="tp-plan" class="menu_button">现在推一条</button>
        <button id="tp-add" class="menu_button">手动加一条</button>
        <button id="tp-import-mc" class="menu_button">从记忆链导入（可选）</button>
        <button id="tp-edit" class="menu_button">编辑 JSON</button>
        <button id="tp-clear" class="menu_button">清空本聊天的线索</button>
      </div>
      <div id="tp-json-wrap" style="display:none">
        <textarea id="tp-json" rows="5"></textarea>
        <button id="tp-save" class="menu_button">保存</button>
      </div>
      <div class="tp-hint">线索来源：① 规则抽取（承诺／疑问／消息类句式，零模型成本）② 可选模型精修 ③ 手动添加。停滞按"轮"计算，换聊天各自独立。</div>
      <div id="tp-warn" class="tp-warn"></div>
    </div>
  </div>
</div>`;
        const containers = ['#extensions_settings', '#extensions_settings2', '#extensions-settings', '#rm_extensions_block'];
        let mounted = null;
        for (const sel of containers) {
            const el = $(sel);
            if (el && el.length) { el.append(html); mounted = sel; break; }
        }
        if (!mounted) {
            const fab = $('<div id="tp-fab" title="多线推进设置">多线</div>');
            const host = $('<div id="tp-float-host" class="thread-pusher-settings"></div>');
            host.html(html);
            $('body').append(fab).append(host);
            fab.on('click', () => host.toggleClass('tp-open'));
            mounted = '#tp-float-host';
        }
        log('面板挂载：' + mounted);
        $panel = $('.thread-pusher-settings').last();

        const bind = (sel, key, type) => {
            const el = $panel.find(sel);
            if (!el.length) return;
            if (type === 'bool') el.prop('checked', !!S.settings[key]); else el.val(S.settings[key]);
            el.on('input change', function () {
                S.settings[key] = type === 'bool' ? $(this).prop('checked') : (type === 'num' ? Number($(this).val()) : $(this).val());
                saveSettings();
            });
        };
        bind('#tp-enabled', 'enabled', 'bool');
        bind('#tp-intensity', 'intensity', 'num');
        bind('#tp-stale', 'staleTurns', 'num');
        bind('#tp-main', 'mainCount', 'num');
        bind('#tp-bg', 'bgCount', 'num');
        bind('#tp-cd', 'cooldownTurns', 'num');
        bind('#tp-every', 'everyTurns', 'num');
        bind('#tp-scan', 'scanTurns', 'num');
        bind('#tp-depth', 'depth', 'num');
        bind('#tp-llm', 'llmExtract', 'bool');
        bind('#tp-llm-every', 'llmEveryTurns', 'num');
        bind('#tp-debug', 'debug', 'bool');

        $panel.find('#tp-scan').on('click', async () => {
            const r = await scan(true);
            toast('扫描完成：台账共 ' + ensure().length + ' 条线索');
        });
        $panel.find('#tp-plan').on('click', () => {
            const r = buildBlock('', hotThreads(''));
            if (!r.block) { toast('没有可推进的线索，先「立即扫描线索」', true); return; }
            const edited = prompt('本轮要推的线索（可改；留空=取消）：', r.block);
            if (edited == null) return;
            inject(edited);
            S.state.pending = r.picked.map((t) => t.id);
            toast('已注入本轮推进指令：' + r.picked.map((t) => t.title).join('；'));
            renderThreads();
        });
        $panel.find('#tp-add').on('click', () => {
            const title = prompt('新线索（一句话）：');
            if (!title) return;
            addThread({ title: title, source: 'manual' });
            persistChat(); renderThreads();
            toast('已添加');
        });
        $panel.find('#tp-import-mc').on('click', () => {
            const mc = (typeof window !== 'undefined') ? window.__memoryChain : null;
            if (!mc || !mc.state) { toast('未检测到「记忆链」扩展；本功能只是可选导入', true); return; }
            let n = 0;
            (mc.state.chapters || []).forEach((c) => { if (c && c.open) { addThread({ title: c.open, who: c.who || [], source: 'mc' }); n++; } });
            persistChat(); renderThreads();
            toast('已从记忆链导入 ' + n + ' 条悬念');
        });
        $panel.find('#tp-edit').on('click', () => {
            const wrap = $panel.find('#tp-json-wrap');
            if (wrap.is(':hidden')) $panel.find('#tp-json').val(JSON.stringify(ensure(), null, 1));
            wrap.toggle();
        });
        $panel.find('#tp-save').on('click', () => {
            try {
                const arr = JSON.parse(String($panel.find('#tp-json').val() || '[]'));
                if (!Array.isArray(arr)) throw new Error('必须是数组');
                arr.forEach((t) => { t.grams = bigrams(t.title || ''); });
                S.threads = arr;
                persistChat(); renderThreads();
                toast('已保存 ' + arr.length + ' 条线索');
            } catch (e) { toast('JSON 解析失败：' + e.message, true); }
        });
        $panel.find('#tp-clear').on('click', () => {
            S.threads = [];
            S.state.pending = [];
            persistChat(); renderThreads();
            toast('已清空本聊天的线索');
        });
        renderThreads();
    }

    function renderThreads() {
        if (!$panel || !$panel.length) return;
        const turn = turnNow();
        const list = ensure();
        const open = list.filter((t) => !t.done);
        const staleN = open.filter((t) => (t.lastSeenTurn ? turn - t.lastSeenTurn : 999) >= (Number(S.settings.staleTurns) || 4)).length;
        const st = $panel.find('#tp-status');
        if (st.length) {
            st.text('存储：' + ({ tauri: 'TauriTavern store', idb: 'IndexedDB', memory: '内存（不持久）' }[Store.kind] || Store.kind) +
                '　轮次 ' + turn +
                '\n线索 ' + list.length + ' 条（未完成 ' + open.length + '，其中停滞 ' + staleN + '）' +
                '　强度 ' + (S.settings.intensity || 0) +
                (S.state.lastPushTurn != null ? '　上次推进于第 ' + S.state.lastPushTurn + ' 轮' : '　尚未推进过'));
        }
        const box = $panel.find('#tp-list');
        if (box.length) {
            box.empty();
            list.slice().sort((a, b) => (a.done ? 1 : 0) - (b.done ? 1 : 0) ||
                ((b.lastSeenTurn ? turn - b.lastSeenTurn : 999) - (a.lastSeenTurn ? turn - a.lastSeenTurn : 999))).slice(0, 14)
                .forEach((t) => {
                    const stale = t.lastSeenTurn ? (turn - t.lastSeenTurn) : '从未';
                    const row = $('<div class="tp-item"></div>');
                    row.append($('<div></div>').text((t.done ? '✔ ' : '· ') + t.title +
                        '　[' + (t.source || '?') + '｜停滞 ' + stale + (typeof stale === 'number' ? ' 轮' : '') + '｜已推 ' + (t.pushed || 0) + ']'));
                    row.append($('<button class="menu_button">推一下</button>').on('click', () => {
                        inject(['【多线推进·手动指定】', '请在本轮回复里推进这条线索：' + t.title + (t.hint ? '（建议：' + t.hint + '）' : ''),
                            '规矩：自然带出、不要一次解决、不替玩家做决定。'].join('\n'));
                        S.state.pending = [t.id];
                        toast('已注入：' + t.title);
                    }));
                    row.append($('<button class="menu_button">完成</button>').on('click', () => { t.done = true; persistChat(); renderThreads(); }));
                    row.append($('<button class="menu_button">删除</button>').on('click', () => {
                        S.threads = ensure().filter((x) => x.id !== t.id); persistChat(); renderThreads();
                    }));
                    box.append(row);
                });
        }
        const w = $panel.find('#tp-warn');
        if (w.length) w.text(Store.kind === 'memory' ? '⚠ ' + (S.storeNote || '当前环境无法持久化，线索仅本次会话有效') : '');
    }

    function toast(msg, isErr) {
        try {
            if (typeof toastr !== 'undefined') toastr[isErr ? 'error' : 'info'](msg, '多线推进');
            else console.log('[多线推进]', msg);
        } catch (e) { console.log('[多线推进]', msg); }
    }

    function saveSettings() {
        try {
            if (ctx.extensionSettings) ctx.extensionSettings[MODULE] = S.settings;
            if (typeof ctx.saveSettingsDebounced === 'function') ctx.saveSettingsDebounced();
            Store.saveKV('settings', S.settings);
        } catch (e) { warn('设置保存失败', e); }
    }

    // ---------------------------------------------------------------- 事件
    function bindEvents() {
        if (!eventSource) return;
        if (event_types.GENERATION_AFTER_COMMANDS) {
            eventSource.on(event_types.GENERATION_AFTER_COMMANDS, () => {
                const chat = ctx.chat || [];
                const lastUser = [...chat].reverse().find((m) => m && m.is_user);
                try { planAndInject(lastUser ? lastUser.mes : ''); } catch (e) { warn('注入失败', e); }
            });
        }
        if (event_types.GENERATION_ENDED) {
            eventSource.on(event_types.GENERATION_ENDED, async () => {
                try { if (S.settings.autoCommit) commit(); } catch (e) { warn('推进记录失败', e); }
                try {
                    const turn = turnNow();
                    const every = Number(S.settings.llmEveryTurns) || 8;
                    const due = S.settings.llmExtract && (turn - (S.state.lastExtractTurn || 0) >= every);
                    await scan(!!due);
                } catch (e) { warn('扫描失败', e); }
            });
        }
        if (event_types.CHAT_CHANGED) {
            eventSource.on(event_types.CHAT_CHANGED, async () => { await loadChatState(); inject(''); renderThreads(); });
        }
        if (event_types.MESSAGE_DELETED) {
            eventSource.on(event_types.MESSAGE_DELETED, () => { S.state.pending = []; });
        }
    }

    async function loadChatState() {
        S.ns = nsKey();
        S.threads = [];
        S.state = { lastPushTurn: null, pending: [], lastExtractTurn: 0 };
        const data = await Store.loadChat(slugify(S.ns));
        if (data && Array.isArray(data.threads)) {
            S.threads = data.threads;
            ensure().forEach((t) => { if (!t.grams) t.grams = bigrams(t.title); t.who = t.who || []; });
            if (data.state && typeof data.state === 'object') Object.assign(S.state, data.state, { pending: [] });
        }
    }

    // ---------------------------------------------------------------- 初始化
    // 幂等：宿主可能多次触发 bootstrap，这里保证只初始化一次，且后来者会 await 同一个 Promise
    let initPromise = null;
    function init() {
        if (!initPromise) initPromise = doInit();
        return initPromise;
    }

    async function doInit() {
        if (!bindContext()) await waitForContext(10000);
        try {
            if (ctx.extensionSettings && ctx.extensionSettings[MODULE]) Object.assign(S.settings, ctx.extensionSettings[MODULE]);
        } catch (e) { /* ignore */ }
        buildUI();
        await Store.init();
        const saved = await Store.loadKV('settings');
        if (saved && typeof saved === 'object') Object.assign(S.settings, saved);
        await loadChatState();
        bindEvents();
        renderThreads();
        console.log('[多线推进] 就绪：存储', Store.kind, '・线索', S.threads.length, '・强度', S.settings.intensity);
    }

    window.__threadPusher = {
        state: S, Store, scan, extractByRules, addThread, ensure, candidates, isPassive, hotThreads,
        shouldPush, buildBlock, planAndInject, commit, persistChat, renderThreads, bigrams, overlap,
        get runState() { return S.state; },          // 运行态：lastPushTurn / pending / lastExtractTurn
        get settings() { return S.settings; }, init,
    };

    if (typeof jQuery !== 'undefined') jQuery(() => init());
    else if (typeof document !== 'undefined' && document.readyState !== 'loading') init();
    else if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded', () => init());
})();
