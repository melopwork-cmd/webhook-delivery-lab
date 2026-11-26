import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { request } from 'node:http';
import { mkdtempSync, readdirSync, rmdirSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { createApi } from '../src/http.ts';
import { deliverOne } from '../src/delivery.ts';
import { DeliveryStore } from '../src/store.ts';

const options = {
  endpoint: new URL('http://127.0.0.1:4000/webhook'),
  secret: 'example-secret-for-tests',
  maxAttempts: 2,
  baseDelayMs: 100,
  timeoutMs: 50,
  leaseMs: 200,
};

test('HTTP ingestion keeps duplicates idempotent and exposes delivery state', async t => {
  const store = new DeliveryStore(':memory:');
  const server = createApi(store);
  t.after(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()));
    store.close();
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert(address && typeof address !== 'string');
  const url = 'http://127.0.0.1:' + address.port + '/events';
  const post = (data: unknown) => fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(data),
  });

  const queueUrl = new URL('/queue/status', url);
  assert.deepEqual(await (await fetch(queueUrl)).json(), { queued: 0, sending: 0, retry: 0, delivered: 0, failed: 0 });
  const event = { id: 'demo-1', payload: { status: 'ready' } };
  assert.equal((await post(event)).status, 202);
  assert.equal((await post(event)).status, 200);
  assert.equal((await post({ ...event, payload: { status: 'changed' } })).status, 409);
  assert.equal((await post({ id: '../unsafe' })).status, 400);
  assert.equal((await post({ ...event, payload: 'x'.repeat(65_536) })).status, 413);
  const chunkedStatus = await new Promise<number | undefined>((resolve, reject) => {
    const outgoing = request(url, { method: 'POST', headers: { 'content-type': 'application/json' } }, reply => {
      reply.resume();
      resolve(reply.statusCode);
    });
    outgoing.on('error', reject);
    outgoing.write('x'.repeat(65_536));
    outgoing.end('x');
  });
  assert.equal(chunkedStatus, 413);
  const status = await fetch(url + '/demo-1');
  assert.equal((await status.json()).state, 'queued');
  assert.deepEqual(await (await fetch(queueUrl)).json(), { queued: 1, sending: 0, retry: 0, delivered: 0, failed: 0 });

  const failedUrl = new URL('/queue/failed', url);
  assert.deepEqual(await (await fetch(failedUrl)).json(), []);
  assert.equal((await fetch(`${failedUrl}?limit=101`)).status, 400);
  for (const [id, when] of [['first', 40], ['second', 50]] as const) {
    store.add(id, '{"private":true}', when);
    const job = store.claim(when, 100, 2);
    assert.equal(job?.id, id);
    assert(job);
    store.finish(job, 'failed', when, 'Test failure');
  }
  const failures = await (await fetch(`${failedUrl}?limit=1`)).json();
  assert.deepEqual(failures, [{ id: 'second', state: 'failed', attempts: 1, due_at: 50, last_error: 'Test failure' }]);
});

test('retry listing shows the earliest retry first', async t => {
  const store = new DeliveryStore(':memory:');
  const server = createApi(store);
  t.after(() => { server.close(); store.close(); });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert(address && typeof address !== 'string');
  const url = 'http://127.0.0.1:' + address.port + '/queue/retries';
  for (const [id, due] of [['later', 500], ['sooner', 100]] as const) {
    store.add(id, '{}', 10);
    const claimed = store.claim(10, 5, 2);
    assert(claimed);
    store.finish(claimed, 'retry', due, 'Temporary failure');
  }
  store.add('queued', '{}', 1);
  const entries = await (await fetch(url + '?limit=1')).json();
  assert.deepEqual(entries, [{ id: 'sooner', state: 'retry', attempts: 1, due_at: 100, last_error: 'Temporary failure' }]);
  assert.equal((await fetch(url + '?limit=0')).status, 400);
});

test('transient responses retry only when due and sign the original payload', async t => {
  const store = new DeliveryStore(':memory:');
  t.after(() => store.close());
  let now = 10_000;
  let calls = 0;
  const payload = '{"event":"created"}';
  store.add('event-1', payload, now);
  const request: typeof fetch = async (url, init) => {
    calls++;
    const headers = new Headers(init?.headers);
    assert.equal(String(url), options.endpoint.href);
    assert.equal(headers.get('x-event-id'), 'event-1');
    assert.equal(init?.body, payload);
    const stamp = headers.get('x-webhook-timestamp');
    const expected = createHmac('sha256', options.secret).update('event-1.' + stamp + '.' + payload).digest('hex');
    assert.equal(headers.get('x-webhook-signature'), 'v1=' + expected);
    return new Response(null, { status: calls === 1 ? 503 : 204 });
  };

  assert.equal(await deliverOne(store, options, () => now, request), true);
  assert.equal(store.status('event-1')?.state, 'retry');
  assert.equal(await deliverOne(store, options, () => now, request), false);
  now += 100;
  assert.equal(await deliverOne(store, options, () => now, request), true);
  assert.equal(store.status('event-1')?.state, 'delivered');
  assert.equal(store.status('event-1')?.attempts, 2);
});

test('retry scheduling honors later receiver deadlines and ignores malformed or past values', async () => {
  const now = 1_790_000_000_000;
  const replies = [
    { code: 429, header: '90', delay: 90_000 },
    { code: 503, header: new Date(now + 120_000).toUTCString(), delay: 120_000 },
    { code: 429, header: '2.5', delay: 100 },
    { code: 503, header: new Date(now - 1000).toUTCString(), delay: 100 },
  ];
  for (const reply of replies) {
    const store = new DeliveryStore(':memory:');
    try {
      store.add('retry', '{}', now);
      await deliverOne(store, options, () => now, async () =>
        new Response(null, { status: reply.code, headers: { 'retry-after': reply.header } }));
      assert.equal(store.status('retry')?.state, 'retry');
      assert.equal(store.status('retry')?.due_at, now + reply.delay);
    } finally {
      store.close();
    }
  }
});

test('HTTP 400 fails immediately and repeated transport errors stop at the attempt limit', async t => {
  const store = new DeliveryStore(':memory:');
  t.after(() => store.close());
  store.add('bad-request', '{}', 0);
  await deliverOne(store, options, () => 0, async () => new Response(null, { status: 400 }));
  assert.equal(store.status('bad-request')?.state, 'failed');
  assert.equal(store.status('bad-request')?.attempts, 1);

  store.add('network-error', '{}', 0);
  const offline = async () => { throw new Error('Network unavailable'); };
  await deliverOne(store, options, () => 0, offline);
  await deliverOne(store, options, () => 100, offline);
  assert.equal(store.status('network-error')?.state, 'failed');
  assert.equal(store.status('network-error')?.attempts, 2);
});

test('the SQLite lease survives restart and prevents a stale worker from finishing', t => {
  const dir = mkdtempSync(join(tmpdir(), 'webhook-lab-'));
  const filename = join(dir, 'queue.db');
  t.after(() => {
    for (const entry of readdirSync(dir)) unlinkSync(join(dir, entry));
    rmdirSync(dir);
  });
  const original = new DeliveryStore(filename);
  original.add('restart', '{"attempt":1}', 0);
  const first = original.claim(0, 200, 3);
  original.close();
  assert(first);

  const restarted = new DeliveryStore(filename);
  try {
    assert.equal(restarted.claim(199, 200, 3), undefined);
    const second = restarted.claim(200, 200, 3);
    assert(second);
    assert.equal(second.payload, '{"attempt":1}');
    assert.equal(second.attempts, 2);
    assert.equal(restarted.finish(first, 'delivered', 220, null), false);
    assert.equal(restarted.finish(second, 'delivered', 220, null), true);
  } finally {
    restarted.close();
  }
});
