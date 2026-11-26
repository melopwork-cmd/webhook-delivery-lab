# Webhook Delivery Lab

A TypeScript service that accepts a JSON event and delivers its payload to a configured webhook. SQLite preserves the queue across restarts. An expiring lease lets a worker recover interrupted deliveries.

## Run the demo

Use Node.js 24 or newer. Runtime code uses Node's HTTP, SQLite, and crypto modules.

~~~text
npm ci
npm run typecheck
npm test
~~~

In one PowerShell window, start the signed test receiver. It returns a simulated 503 on the first delivery for each ID.

~~~powershell
$env:WEBHOOK_SECRET = 'local-demo-secret-change-me'
$env:DEMO_FAIL_FIRST = '1'
npm run demo
~~~

In a second window, start the API with the same demo secret.

~~~powershell
$env:WEBHOOK_SECRET = 'local-demo-secret-change-me'
$env:WEBHOOK_DESTINATION = 'http://127.0.0.1:4000/webhook'
npm start
~~~

In a third window, queue an event and check its status after a few seconds.

~~~powershell
$event = @{ id = 'order-123'; payload = @{ status = 'paid' } } | ConvertTo-Json -Compress
Invoke-RestMethod http://127.0.0.1:3000/events -Method Post -Body $event -ContentType 'application/json'
Invoke-RestMethod http://127.0.0.1:3000/events/order-123
~~~

The event should reach delivered after two attempts. Choose a new ID on subsequent runs or change DB_PATH to use a new queue.

## Delivery behavior

- Reusing an event ID with the same serialized payload returns the existing entry. Reusing it with a different payload returns HTTP 409.
- Timeouts, connection errors, HTTP 408 and 429, and 5xx responses retry with capped exponential backoff. Other errors fail immediately. If a retryable response includes a valid Retry-After value, the worker waits until the later of the receiver's deadline and its normal backoff.
- A delivery is leased with a unique claim token. A restarted worker can reclaim an expired lease, and a stale worker cannot update the newer claim.
- Delivery is at least once. A receiver can use X-Event-Id to suppress duplicates after a delivered response is lost.
- The signature is HMAC-SHA256 over the event ID, Unix timestamp, and exact request body, joined with periods. The included receiver verifies the signature and checks timestamp freshness.

The service listens on loopback and sends to one operator-configured destination. This example is meant for local development and interviews. A shared deployment would need authenticated ingestion, durable deduplication at the receiver, metrics, and data retention controls.

### References

- [Node.js SQLite API](https://nodejs.org/api/sqlite.html)
- [RFC 2104 on HMAC](https://www.rfc-editor.org/rfc/rfc2104)

Independent portfolio project, built with AI assistance.
