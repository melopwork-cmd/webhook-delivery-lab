import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { DeliveryStore } from './store.ts';

const LIMIT = 64 * 1024;
const ID = /^[A-Za-z0-9_-]{1,80}$/;

function reply(res: ServerResponse, code: number, data: unknown): void {
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(data));
}

async function readBody(req: IncomingMessage): Promise<Buffer | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += bytes.length;
    if (size <= LIMIT) chunks.push(bytes);
  }
  return size > LIMIT ? null : Buffer.concat(chunks, size);
}

export function createApi(store: DeliveryStore) {
  return createServer({ requestTimeout: 10_000 }, async (req, res) => {
    try {
      const path = (req.url ?? '/').split('?')[0];
      if (req.method === 'GET' && path === '/health') {
        return reply(res, 200, { status: 'ok' });
      }

      if (req.method === 'GET' && path.startsWith('/events/')) {
        const id = path.slice('/events/'.length);
        const entry = ID.test(id) ? store.status(id) : undefined;
        return entry ? reply(res, 200, entry) : reply(res, 404, { error: 'Event not found' });
      }

      if (req.method !== 'POST' || path !== '/events') {
        return reply(res, 404, { error: 'Route not found' });
      }
      if (!/^application\/json(?:;|$)/i.test(req.headers['content-type'] ?? '')) {
        req.resume();
        return reply(res, 415, { error: 'Send application/json' });
      }

      const raw = await readBody(req);
      if (!raw) return reply(res, 413, { error: 'Body exceeds 64 KiB' });
      let event: unknown;
      try {
        event = JSON.parse(raw.toString('utf8'));
      } catch {
        return reply(res, 400, { error: 'Malformed JSON' });
      }
      if (typeof event !== 'object' || event === null || !('id' in event) ||
          typeof event.id !== 'string' || !ID.test(event.id) || !('payload' in event)) {
        return reply(res, 400, { error: 'Provide a valid id and a JSON payload' });
      }
      const status = store.add(event.id, JSON.stringify(event.payload), Date.now());
      return status === 'conflict'
        ? reply(res, 409, { error: 'The event id already exists with a different payload' })
        : reply(res, status === 'created' ? 202 : 200, { id: event.id, status });
    } catch {
      if (!res.headersSent && !res.destroyed) reply(res, 500, { error: 'Could not process the request' });
    }
  });
}
