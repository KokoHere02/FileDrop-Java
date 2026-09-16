// Node.js 22+, Java 17+, and a local Chromium/Edge executable; no npm packages required.
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { readFile, mkdir, mkdtemp, writeFile, access } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const output = resolve(root, 'target/network-e2e');
await mkdir(output, { recursive: true });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const children = [];
const browsers = [];
const results = [];
let backendLog = '';
let fixture;
async function eventually(check, label, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await sleep(100);
  }
  throw new Error(`Timed out: ${label}`);
}
function launch(executable, args) {
  const child = spawn(executable, args, { cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  child.on('error', error => { child.failure = error; });
  children.push(child);
  return child;
}
class CDP {
  constructor(ws) {
    this.ws = ws; this.next = 0; this.pending = new Map();
    ws.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id); clearTimeout(pending.timer);
      if (message.error) pending.reject(new Error(message.error.message));
      else pending.resolve(message.result);
    });
    ws.addEventListener('close', () => {
      for (const pending of this.pending.values()) {
        clearTimeout(pending.timer); pending.reject(new Error('Browser connection closed'));
      }
      this.pending.clear();
    });
  }
  call(method, params = {}, sessionId) {
    const id = ++this.next;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 20000);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  }
}
async function openBrowser(executable) {
  const profile = await mkdtemp(resolve(output, 'profile-'));
  const process = launch(executable, ['--headless=new', '--no-first-run', '--no-default-browser-check',
    '--disable-background-timer-throttling', '--disable-renderer-backgrounding',
    '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank']);
  process.stdout.on('data', () => {});
  process.stderr.on('data', () => {});
  const endpoint = await eventually(async () => {
    if (process.failure) throw process.failure;
    try {
      const [port, path] = (await readFile(resolve(profile, 'DevToolsActivePort'), 'utf8')).trim().split(/\r?\n/);
      return `ws://127.0.0.1:${port}${path}`;
    } catch { return null; }
  }, 'browser startup');
  const ws = new WebSocket(endpoint);
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, { once: true }); ws.addEventListener('error', reject, { once: true }); });
  const cdp = new CDP(ws);
  browsers.push(cdp);
  return cdp;
}
async function page(cdp, origin) {
  const { targetId } = await cdp.call('Target.createTarget', { url: origin });
  const { sessionId } = await cdp.call('Target.attachToTarget', { targetId, flatten: true });
  const evaluate = async expression => {
    const result = await cdp.call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
    return result.result.value;
  };
  await eventually(() => evaluate('typeof window.start === "function"'), 'fixture load');
  return { evaluate, close: () => cdp.call('Target.closeTarget', { targetId }) };
}
async function waitState(peer, predicate, label) {
  return eventually(async () => {
    const state = await peer.evaluate('window.state');
    assert.deepEqual(state.errors, [], `${label}: browser errors`);
    return predicate(state) && state;
  }, label);
}
async function transfer(sender, receiver, size) {
  const expected = await sender.evaluate(`window.sendFile(${size})`);
  const received = await waitState(receiver, s => s.received?.size === size, 'file reception');
  const ack = await waitState(sender, s => s.ack?.size === size, 'file acknowledgement');
  assert.deepEqual(received.received, expected);
  assert.equal(ack.ack.sha256, expected.sha256);
  return expected;
}
try {
  const candidates = [process.env.BROWSER_EXECUTABLE,
    'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    'C:/Program Files/Google/Chrome/Application/chrome.exe', '/usr/bin/chromium', '/usr/bin/google-chrome'].filter(Boolean);
  let executable;
  for (const candidate of candidates) { try { await access(candidate); executable = candidate; break; } catch {} }
  if (!executable) throw new Error('Set BROWSER_EXECUTABLE to a Chromium/Edge executable');
  const jar = resolve(root, 'target/FileDrop-Java-1.0.jar');
  await access(jar);
  fixture = createServer(async (request, response) => {
    const name = request.url === '/peer.js' ? 'peer.js' : 'peer.html';
    try {
      response.setHeader('Content-Type', name.endsWith('.js') ? 'text/javascript' : 'text/html');
      response.end(await readFile(resolve(root, 'src/test/browser', name)));
    } catch { response.writeHead(500); response.end(); }
  });
  await new Promise(resolve => fixture.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${fixture.address().port}`;
  const backend = launch(process.env.JAVA_EXECUTABLE ?? 'java', ['-jar', jar, '--server.port=0',
    `--app.allowed-origins=${origin}`, '--app.heartbeat.interval-ms=1000', '--app.heartbeat.timeout-ms=5000', '--app.heartbeat.scan-ms=100']);
  backend.stdout.on('data', data => { backendLog += data; });
  backend.stderr.on('data', data => { backendLog += data; });
  const port = await eventually(() => {
    if (backend.failure) throw backend.failure;
    if (backend.exitCode !== null) throw new Error('Backend exited before startup');
    return backendLog.match(/Tomcat started on port (\d+)/)?.[1];
  }, 'backend startup', 30000);
  const base = `http://127.0.0.1:${port}/api`;
  const firstBrowser = await openBrowser(executable);
  const secondBrowser = await openBrowser(executable);
  const version = await firstBrowser.call('Browser.getVersion');
  for (const firstRole of ['sender', 'receiver']) {
    let sender = await page(firstBrowser, origin);
    const receiver = await page(secondBrowser, origin);
    // Create via browser fetch, exercising real HTTP plus browser CORS.
    const created = await sender.evaluate(`fetch(${JSON.stringify(base + '/web/createRoom?type=file')}, { method: 'POST' })
      .then(async r => { if (!r.ok) throw new Error('Create HTTP ' + r.status); return (await r.json()).data; })`);
    const connect = (peer, role) => peer.evaluate(`window.start(${JSON.stringify({ base, code: created.code, role,
      ...(role === 'sender' ? { senderToken: created.senderToken } : {}) })})`);
    const first = firstRole === 'sender' ? sender : receiver;
    const second = firstRole === 'sender' ? receiver : sender;
    await connect(first, firstRole);
    await waitState(first, s => s.accepted, 'first accepted');
    await connect(second, firstRole === 'sender' ? 'receiver' : 'sender');
    const states = await Promise.all([waitState(first, s => s.open, 'first channel open'), waitState(second, s => s.open, 'second channel open')]);
    assert.equal(states[0].initiator, firstRole === 'sender');
    assert.equal(states[1].initiator, firstRole !== 'sender');
    const file = await transfer(sender, receiver, 1024 * 1024 + 123);
    results.push({ scenario: `${firstRole}-first`, ...file, passed: true });
    if (firstRole === 'sender') {
      await sender.close(); // Close the actual page/transport without sending the fixture's stop command.
      await waitState(receiver, s => s.resets === 1, 'peer reset after page termination');
      sender = await page(firstBrowser, origin);
      await connect(sender, 'sender');
      await Promise.all([waitState(sender, s => s.open, 'sender reconnect'), waitState(receiver, s => s.open, 'receiver renegotiation')]);
      results.push({ scenario: 'reconnect-transfer', ...await transfer(sender, receiver, 128 * 1024 + 7), passed: true });
    }
    await sender.close(); await receiver.close();
    console.log(`PASS ${firstRole}-first file transfer`);
  }
  const thirdBrowser = await openBrowser(executable);
  const sender = await page(firstBrowser, origin);
  const receiverA = await page(secondBrowser, origin);
  const receiverB = await page(thirdBrowser, origin);
  const created = await sender.evaluate(`fetch(${JSON.stringify(base + '/web/createRoom?type=file')}, { method: 'POST' }).then(r => r.json()).then(r => r.data)`);
  const connectMany = (peer, role) => peer.evaluate(`window.start(${JSON.stringify({ base, code: created.code, role,
    ...(role === 'sender' ? { senderToken: created.senderToken } : {}) })})`);
  await connectMany(receiverA, 'receiver');
  await connectMany(receiverB, 'receiver');
  await Promise.all([waitState(receiverA, s => s.accepted, 'A accepted'), waitState(receiverB, s => s.accepted, 'B accepted')]);
  await connectMany(sender, 'sender');
  await waitState(sender, s => Object.values(s.peers).filter(p => p.open).length === 2, 'two receiver channels');
  const broadcast = await sender.evaluate('window.sendFile(2097275)');
  for (const receiver of [receiverA, receiverB]) {
    const state = await waitState(receiver, s => s.received?.size === broadcast.size, 'broadcast reception');
    assert.deepEqual(state.received, broadcast);
  }
  await waitState(sender, s => Object.keys(s.acks).length === 2, 'both receivers acknowledged');
  results.push({ scenario: 'one-sender-two-receivers', ...broadcast, passed: true });
  await receiverA.close();
  await waitState(sender, s => s.resets === 1 && Object.keys(s.peers).length === 1, 'A removed independently');
  assert.equal((await receiverB.evaluate('window.state')).resets, 0);
  results.push({ scenario: 'remaining-receiver-transfer', ...await transfer(sender, receiverB, 262151), passed: true });
  const late = await page(secondBrowser, origin);
  await connectMany(late, 'receiver');
  const lateState = await waitState(late, s => s.received?.size === 262151, 'late receiver full replay');
  assert.deepEqual(lateState.received, (await receiverB.evaluate('window.state')).received);
  await waitState(sender, s => Object.keys(s.acks).length === 2, 'late receiver acknowledgement');
  assert.equal((await receiverB.evaluate('window.state')).resets, 0);
  results.push({ scenario: 'late-receiver-from-start', ...lateState.received, passed: true });
  await sender.close(); await receiverB.close(); await late.close();
  await writeFile(resolve(output, 'result.json'), JSON.stringify({ passed: true, browser: version.product, scenarios: results }, null, 2));
  console.log('PASS: three browser processes, broadcast, receiver isolation, late join, SHA-256 and reconnect');
} catch (error) {
  await writeFile(resolve(output, 'result.json'), JSON.stringify({ passed: false, error: String(error), scenarios: results }, null, 2));
  console.error(String(error)); process.exitCode = 1;
} finally {
  for (const browser of browsers) {
    try { await browser.call('Browser.close'); } catch {}
    browser.ws.close();
  }
  for (const child of children) if (child.exitCode === null) child.kill();
  if (fixture) fixture.closeAllConnections();
  if (fixture) await new Promise(resolve => fixture.close(resolve));
  await writeFile(resolve(output, 'backend.log'), backendLog);
}
