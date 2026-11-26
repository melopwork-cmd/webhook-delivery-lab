import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

export type DeliveryState = 'queued' | 'sending' | 'retry' | 'delivered' | 'failed';
export type DeliveryStatus = {
  id: string;
  state: DeliveryState;
  attempts: number;
  due_at: number;
  last_error: string | null;
};
export type ClaimedDelivery = {
  id: string;
  payload: string;
  attempts: number;
  lease_token: string;
};

export class DeliveryStore {
  private readonly db: DatabaseSync;

  constructor(filename: string) {
    this.db = new DatabaseSync(filename);
    this.db.exec(
      [
        'PRAGMA journal_mode = WAL;',
        'PRAGMA busy_timeout = 2000;',
        'CREATE TABLE IF NOT EXISTS deliveries (',
        '  id TEXT PRIMARY KEY, payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,',
        "  state TEXT NOT NULL CHECK (state IN ('queued','sending','retry','delivered','failed')),",
        '  due_at INTEGER NOT NULL, lease_until INTEGER, lease_token TEXT, last_error TEXT',
        ');',
        'CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(state, due_at);',
      ].join('\n'),
    );
  }

  add(id: string, payload: string, now: number): 'created' | 'duplicate' | 'conflict' {
    const result = this.db.prepare(
      "INSERT OR IGNORE INTO deliveries (id, payload, state, due_at) VALUES (?, ?, 'queued', ?)",
    ).run(id, payload, now);
    if (result.changes) return 'created';
    const stored = this.db.prepare('SELECT payload FROM deliveries WHERE id = ?').get(id);
    return stored?.payload === payload ? 'duplicate' : 'conflict';
  }

  counts(): Record<DeliveryState, number> {
    const counts = { queued: 0, sending: 0, retry: 0, delivered: 0, failed: 0 };
    const rows = this.db.prepare('SELECT state, COUNT(*) AS total FROM deliveries GROUP BY state').all();
    for (const row of rows) counts[row.state as DeliveryState] = Number(row.total);
    return counts;
  }

  nextDue(): number | null {
    const row = this.db.prepare(
      "SELECT MIN(due_at) AS earliest FROM deliveries WHERE state IN ('queued', 'retry')",
    ).get();
    return row?.earliest == null ? null : Number(row.earliest);
  }

  failed(limit: number): DeliveryStatus[] {
    return this.db.prepare(
      "SELECT id, state, attempts, due_at, last_error FROM deliveries WHERE state = 'failed' " +
      'ORDER BY due_at DESC, id ASC LIMIT ?',
    ).all(limit) as DeliveryStatus[];
  }

  retries(limit: number): DeliveryStatus[] {
    return this.db.prepare(
      "SELECT id, state, attempts, due_at, last_error FROM deliveries WHERE state = 'retry' " +
      'ORDER BY due_at, id LIMIT ?',
    ).all(limit) as DeliveryStatus[];
  }

  status(id: string): DeliveryStatus | undefined {
    return this.db.prepare('SELECT id, state, attempts, due_at, last_error FROM deliveries WHERE id = ?')
      .get(id) as DeliveryStatus | undefined;
  }

  claim(now: number, leaseMs: number, maxAttempts: number): ClaimedDelivery | undefined {
    this.db.prepare(
      "UPDATE deliveries SET state = 'failed', lease_until = NULL, lease_token = NULL, last_error = 'Delivery lease expired' " +
      "WHERE state = 'sending' AND lease_until <= ? AND attempts >= ?",
    ).run(now, maxAttempts);

    return this.db.prepare(
      "UPDATE deliveries SET state = 'sending', attempts = attempts + 1, lease_until = ?, lease_token = ? " +
      'WHERE id = (' +
      ' SELECT id FROM deliveries WHERE attempts < ? AND (' +
      "  (state IN ('queued', 'retry') AND due_at <= ?) OR (state = 'sending' AND lease_until <= ?)" +
      ' ) ORDER BY due_at, id LIMIT 1' +
      ') RETURNING id, payload, attempts, lease_token',
    ).get(now + leaseMs, randomUUID(), maxAttempts, now, now) as ClaimedDelivery | undefined;
  }

  finish(
    delivery: ClaimedDelivery,
    state: 'retry' | 'delivered' | 'failed',
    dueAt: number,
    error: string | null,
  ): boolean {
    const result = this.db.prepare(
      'UPDATE deliveries SET state = ?, due_at = ?, last_error = ?, lease_until = NULL, lease_token = NULL ' +
      "WHERE id = ? AND state = 'sending' AND lease_token = ?",
    ).run(state, dueAt, error, delivery.id, delivery.lease_token);
    return Boolean(result.changes);
  }

  close(): void {
    this.db.close();
  }
}
