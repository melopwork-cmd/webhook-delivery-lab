import { createApi } from './http.ts';
import { deliverOne } from './delivery.ts';
import { DeliveryStore } from './store.ts';

const destination = process.env.WEBHOOK_DESTINATION;
const secret = process.env.WEBHOOK_SECRET;
const port = Number(process.env.PORT ?? 3000);

if (!destination || !secret || secret.length < 16) {
  throw new Error('Set WEBHOOK_DESTINATION and a WEBHOOK_SECRET of at least 16 characters');
}
if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new Error('PORT must be a valid port');
const endpoint = new URL(destination);
const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname);
if ((endpoint.protocol !== 'https:' && !(endpoint.protocol === 'http:' && loopback)) ||
    endpoint.username || endpoint.password) {
  throw new Error('Use HTTPS or a local HTTP destination with no embedded credentials');
}

const store = new DeliveryStore(process.env.DB_PATH ?? 'deliveries.db');
const options = { endpoint, secret, maxAttempts: 4, baseDelayMs: 1_000, timeoutMs: 5_000, leaseMs: 12_000 };
const api = createApi(store);
let running: Promise<boolean> | undefined;

const timer = setInterval(() => {
  if (running) return;
  running = deliverOne(store, options);
  void running.catch(error => console.error('Delivery worker error:', error.message)).finally(() => {
    running = undefined;
  });
}, 250);

let stopping = false;
const stop = () => {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  api.close(() => {
    void (running ?? Promise.resolve()).catch(() => {}).finally(() => store.close());
  });
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
api.listen(port, '127.0.0.1', () => console.log('Webhook delivery API at http://127.0.0.1:' + port));
