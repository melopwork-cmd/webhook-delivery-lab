import { timingSafeEqual } from 'node:crypto';
import { createServer } from 'node:http';
import { sign } from '../src/delivery.ts';

const secret = process.env.WEBHOOK_SECRET;
if (!secret) throw new Error('Set WEBHOOK_SECRET to match the delivery service');
const seen = new Set<string>();
const failedOnce = new Set<string>();

const server = createServer({ requestTimeout: 10_000 }, async (req, res) => {
  if (req.method !== 'POST' || req.url !== '/webhook') {
    res.writeHead(404).end();
    return;
  }
  try {
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of req) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      size += bytes.length;
      if (size <= 64 * 1024) chunks.push(bytes);
    }
    if (size > 64 * 1024) {
      res.writeHead(413).end();
      return;
    }
    const payload = Buffer.concat(chunks).toString('utf8');
    const time = req.headers['x-webhook-timestamp'];
    const signature = req.headers['x-webhook-signature'];
    const id = req.headers['x-event-id'];
    if (typeof id !== 'string' || typeof time !== 'string' || typeof signature !== 'string' ||
        !/^\d+$/.test(time) || Math.abs(Date.now() / 1000 - Number(time)) > 300) {
      res.writeHead(401).end();
      return;
    }
    const expected = Buffer.from(sign(secret, id, time, payload));
    const received = Buffer.from(signature);
    if (received.length !== expected.length || !timingSafeEqual(received, expected)) {
      res.writeHead(401).end();
      return;
    }

    if (process.env.DEMO_FAIL_FIRST === '1' && !failedOnce.has(id)) {
      failedOnce.add(id);
      console.log('Simulated HTTP 503 for', id);
      res.writeHead(503).end();
      return;
    }
    if (!seen.has(id)) {
      seen.add(id);
      console.log('Accepted event', id);
    }
    res.writeHead(204).end();
  } catch {
    if (!res.headersSent && !res.destroyed) res.writeHead(400).end();
  }
});
server.listen(4000, '127.0.0.1', () => console.log('Demo receiver at http://127.0.0.1:4000/webhook'));
