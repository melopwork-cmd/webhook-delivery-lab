import { createHmac } from 'node:crypto';
import type { DeliveryStore } from './store.ts';

export type DeliveryOptions = {
  endpoint: URL;
  secret: string;
  maxAttempts: number;
  baseDelayMs: number;
  timeoutMs: number;
  leaseMs: number;
};

export function sign(secret: string, id: string, timestamp: string, payload: string): string {
  return 'v1=' + createHmac('sha256', secret).update(id + '.' + timestamp + '.' + payload).digest('hex');
}

function retryDeadline(value: string | null, receivedAt: number): number {
  const header = value?.trim() ?? '';
  if (/^\d+$/.test(header)) {
    const deadline = receivedAt + Number(header) * 1000;
    return Number.isSafeInteger(deadline) ? deadline : 0;
  }
  const httpDate = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*[ ,]/;
  const date = header.includes(',') ? header : header + ' GMT';
  const deadline = httpDate.test(header) ? Date.parse(date) : NaN;
  return Number.isFinite(deadline) ? deadline : 0;
}

export async function deliverOne(
  store: DeliveryStore,
  options: DeliveryOptions,
  now: () => number = Date.now,
  request: typeof fetch = fetch,
): Promise<boolean> {
  if (options.leaseMs <= options.timeoutMs) throw new Error('Delivery lease must exceed request timeout');
  const event = store.claim(now(), options.leaseMs, options.maxAttempts);
  if (!event) return false;

  const timestamp = String(Math.floor(now() / 1000));
  let failure: string;
  let retryable = true;
  let requestedDeadline = 0;
  try {
    const result = await request(options.endpoint, {
      method: 'POST',
      redirect: 'error',
      signal: AbortSignal.timeout(options.timeoutMs),
      body: event.payload,
      headers: {
        'content-type': 'application/json',
        'x-event-id': event.id,
        'x-webhook-timestamp': timestamp,
        'x-webhook-signature': sign(options.secret, event.id, timestamp, event.payload),
      },
    });
    await result.body?.cancel().catch(() => {});
    if (result.ok) {
      store.finish(event, 'delivered', now(), null);
      return true;
    }
    failure = 'Receiver returned HTTP ' + result.status;
    retryable = result.status === 408 || result.status === 429 || result.status >= 500;
    if (retryable) requestedDeadline = retryDeadline(result.headers.get('retry-after'), now());
  } catch {
    failure = 'Receiver request failed or timed out';
  }

  if (!retryable || event.attempts >= options.maxAttempts) {
    store.finish(event, 'failed', now(), failure);
  } else {
    const delay = Math.min(60_000, options.baseDelayMs * 2 ** (event.attempts - 1));
    store.finish(event, 'retry', Math.max(now() + delay, requestedDeadline), failure);
  }
  return true;
}
