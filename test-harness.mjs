/**
 * 多线推进 ThreadPusher 桩测试（不依赖 TauriTavern / SillyTavern / 世界书）
 *   node thread-pusher/test-harness.mjs
 * 重点验证：没有世界书、没有词表也能跑。
 */
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';

const here = path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
const code = fs.readFileSync(path.join(here, 'index.js'), 'utf8');

let pass = 0, fail = 0;
const ok = (cond, label, extra = '') => {
    if (cond) { pass++; console.log('  ✓ ' + label + (extra ? '  ' + extra : '')); }
    else { fail++; console.log('  ✗ ' + label + '  ' + extra); }
};

// 一个"没有世界书、没有词表"的沙箱
function makeSandbox(opts = {}) {
    const chat = opts.chat || [];
    const injected = {};
    const store = opts.store || makeTauriStore();       // 可传入同一个 store 实例来模拟"重启"
    const sb = {
        console, performance, setTimeout,
        requestIdleCallback: (fn) => setTimeout(fn, 0),
        document: { readyState: 'complete', addEventListener() {}, createElement: () => ({ click() {}, style: {} }) },
        SillyTavern: {
            getContext: () => ({
                chat,
                characters: [{ name: '今山' }], characterId: 0, chatId: opts.chatId || 'story-1',
                name1: '我', name2: '苓公子', eventSource: null, event_types: {}, extensionSettings: {},
                chatMetadata: {},                                  // 注意：没有任何世界书
                extension_prompt_types: { IN_PROMPT: 0, IN_CHAT: 1, BEFORE_PROMPT: 2 },
                extension_prompt_roles: { SYSTEM: 0, USER: 1, ASSISTANT: 2 },
                setExtensionPrompt: (k, v) => { injected[k] = v; },
                generateQuietPrompt: opts.generateQuietPrompt || undefined,
            }),
        },
        __TAURITAVERN__: { ready: Promise.resolve(true), api: { extension: { store } } },
    };
    sb.window = sb; sb.globalThis = sb;
    sb.__injected = injected;
    sb.__store = store;
    sb.__chat = chat;
    vm.createContext(sb);
    vm.runInContext(code, sb, { filename: 'index.js' });
    return sb;
}

function makeTauriStore() {
    const files = new Map();
    const key = (o) => `${o.namespace}|${o.table || 'main'}|${o.key}`;
    const chk = (s) => { if (!/^[A-Za-z0-9_.-]+$/.test(s) || s.startsWith('.')) throw new Error('bad name: ' + s); };
    return {
        calls: 0, files,
        async setJson(o) { chk(o.namespace); chk(o.table || 'main'); chk(o.key); this.calls++; files.set(key(o), JSON.parse(JSON.stringify(o.value))); },
        async getJson(o) { this.calls++; if (!files.has(key(o))) throw new Error('not found'); return files.get(key(o)); },
        async tryGetJson(o) { this.calls++; return files.has(key(o)) ? { found: true, value: files.get(key(o)) } : { found: false }; },
        async deleteJson(o) { this.calls++; files.delete(key(o)); },
        async listKeys(o) { const p = `${o.namespace}|${o.table || 'main'}|`; return [...files.keys()].filter((k) => k.startsWith(p)).map((k) => k.slice(p.length)); },
        async listTables() { return []; },
        async deleteTable(o) { const p = `${o.namespace}|${o.table}|`; for (const k of [...files.keys()]) if (k.startsWith(p)) files.delete(k); },
    };
}

// ---------------- 1 规则抽取（零模型） ----------------
console.log('\n[1] 规则抽取线索（不需要世界书）');
const chat = [
    { is_user: false, name: '苓公子', mes: '师海客说他下次一定把那卷港图带来给苓公子看。' },
    { is_user: true, name: '我', mes: '嗯' },
    { is_user: false, name: '元秉烛', mes: '他嘀咕了一句：那格不在册的书架里到底是什么？' },
    { is_user: true, name: '我', mes: '好' },
    { is_user: false, name: '薄九章', mes: '今年峰的雾比往年重，尚未查明原因。' },
];
const sb1 = makeSandbox({ chat });
const P = sb1.window.__threadPusher;
await P.init();
ok(!!P, '扩展在无世界书环境下正常加载');
ok(P.state.threads.length === 0, '初始台账为空');
let r = await P.scan(false);
const T = P.ensure();
ok(T.length >= 2, '规则抽取出多条线索', T.length + ' 条：' + T.map((t) => t.title.slice(0, 14)).join(' / '));
ok(T.some((t) => /港图/.test(t.title)), '抽到"承诺类"线索（港图）');
ok(T.some((t) => /书架/.test(t.title) || /原因/.test(t.title)), '抽到"疑问/未查明类"线索');
ok(T.every((t) => t.grams && t.grams.size > 0), '每条线索都带匹配用 2-gram');

// 重复扫描不应重复建条目
const before = T.length;
await P.scan(false);
ok(P.ensure().length === before, '重复扫描不会重复建条目', before + ' → ' + P.ensure().length);

// ---------------- 2 被动判定与热度 ----------------
console.log('\n[2] 被动判定 / 谁在被提');
const gk = P.ensure().find((t) => /港图/.test(t.title));
ok(P.isPassive('嗯', P.hotThreads('嗯')) === true, '短消息 → 被动');
ok(P.isPassive('我想想，先看看别的地方再说吧', P.hotThreads('我想想，先看看别的地方再说吧')) === true, '与所有线索无关 → 被动');
const hotText = '那卷港图他到底带来了没有，苓公子还一直等着看呢';
const hot = P.hotThreads(hotText);
ok(hot.indexOf(gk.id) >= 0, '用户提到某条线索 → 该线索被标为"热"');
ok(P.isPassive(hotText, hot) === false, '提到线索 → 非被动');

// ---------------- 3 停滞 → 注入 ----------------
console.log('\n[3] 停滞检测与注入');
for (let i = 0; i < 6; i++) chat.push({ is_user: true, name: '我', mes: '嗯' });   // 推进 6 轮
const cands = P.candidates();
ok(cands.length > 0 && cands[0].stale >= 4, '停滞按"轮"计算', '最停滞 ' + cands[0].stale + ' 轮');
P.settings.intensity = 1;
ok(P.shouldPush('嗯', []) === true, '强度1 + 被动 → 应当推进');
const blk = P.buildBlock('嗯', []);
ok(blk.block.includes('【多线推进·本轮任务】'), '注入块带任务头');
ok(blk.block.includes('不要顺手把别的线也解决') && blk.block.includes('不替玩家做决定'), '注入块带约束');
ok(blk.picked.length >= 1, '选中至少一条', blk.picked.map((t) => t.title.slice(0, 12)).join(' / '));

const planned = P.planAndInject('嗯');
ok(!!sb1.__injected['threadPusher'] && sb1.__injected['threadPusher'].includes('多线推进'), '指令注入到独立槽位 threadPusher');
const mainT = planned.picked[0];
const pushedBefore = mainT.pushed;
P.commit();
ok(mainT.pushed === pushedBefore + 1, '推进次数 +1');
ok(P.runState.lastPushTurn === chat.filter((m) => m.is_user).length, '记录上次推进轮次',
    'lastPushTurn=' + P.runState.lastPushTurn + ' / 用户消息数=' + chat.filter((m) => m.is_user).length);
ok(P.shouldPush('嗯', []) === false, '刚推过 → 不会连续两轮触发');
if (planned.picked[1]) {
    ok(planned.picked[1].cooldownUntilTurn > P.runState.lastPushTurn, '背景线索进入冷却',
        'cd=' + planned.picked[1].cooldownUntilTurn + ' > ' + P.runState.lastPushTurn);
    ok(!P.candidates().some((c) => c.t.id === planned.picked[1].id), '冷却中的线索不再入选');
}

// ---------------- 4 可选：模型精修 ----------------
console.log('\n[4] 可选模型精修（默认关闭）');
const modelReply = JSON.stringify([
    { title: '郝素问在偷偷查三十年前的修女名册', who: ['郝素问'], hint: '让她查到一条被划掉的名字', done: false },
    { title: gk.title, done: true },
]);
const sb2 = makeSandbox({
    chat: chat.slice(),
    generateQuietPrompt: async () => modelReply,
});
const P2 = sb2.window.__threadPusher;
await P2.init();
P2.settings.llmExtract = true;
await P2.scan(true);
const T2 = P2.ensure();
ok(T2.some((t) => /修女名册/.test(t.title)), '模型抽取的新线索已入台账');
ok(T2.some((t) => /港图/.test(t.title) && t.done === true), '模型判定"已解决"的线索被标记完成');
ok(P2.ensure().filter((t) => /港图/.test(t.title)).length === 1, '模型返回与已有线索重复时不会新建');

// ---------------- 5 持久化与聊天隔离 ----------------
console.log('\n[5] 持久化与聊天隔离');
const store = sb2.__store;
const sb3 = makeSandbox({ chat: chat.slice(), chatId: 'story-1', store: store });   // 同一个 store = "重启"
const P3 = sb3.window.__threadPusher;
await P3.init();
ok(P3.ensure().length > 0, '重启后线索台账恢复', P3.ensure().length + ' 条');
const sb4 = makeSandbox({ chat: [], chatId: 'another-story', store: store });
const P4 = sb4.window.__threadPusher;
await P4.init();
ok(P4.ensure().length === 0, '换聊天 → 空台账（按聊天隔离）');

// ---------------- 6 关闭时不干扰 ----------------
console.log('\n[6] 关闭与空台账的边界');
P.settings.intensity = 0;
ok(P.planAndInject('嗯') === null && sb1.__injected['threadPusher'] === '', '强度0 → 不注入并清空槽位');
P.settings.intensity = 1;
P.state.pending = [];
const sb5 = makeSandbox({ chat: [] });
const P5 = sb5.window.__threadPusher;
await P5.init();
ok(P5.planAndInject('嗯') === null, '没有线索时不注入');
ok(P5.buildBlock('嗯', []).block === '', '空台账 → 空指令块');

console.log('\n结果：' + pass + ' 通过 / ' + fail + ' 失败');
process.exit(fail ? 1 : 0);
