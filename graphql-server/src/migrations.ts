import { existsSync, readFileSync, readdirSync } from 'fs';
import { join, resolve } from 'path';
import type { Pool, PoolClient } from 'pg';

export interface Migration { version: string; sql: string; }

export function loadMigrations(directory?: string): Migration[] {
  directory ??= [resolve(__dirname, '../../db/migrations'), resolve(__dirname, '../db/migrations')].find(existsSync)
    ?? resolve(process.cwd(), 'db/migrations');
  return readdirSync(directory).filter(name => /^\d+_[a-z0-9_-]+\.sql$/i.test(name)).sort().map(name => {
    let sql = readFileSync(join(directory, name), 'utf8');
    if (name === '001_init.sql') sql = sql.replace('\\ir ../schema.sql', readFileSync(resolve(directory, '../schema.sql'), 'utf8'));
    sql = sql.replace(/^\\echo.*$/gm, '');
    return { version: name.replace(/\.sql$/, ''), sql };
  });
}

export async function migrationStatus(pool: Pick<Pool, 'query'>, migrations: Migration[]) {
  const result = await pool.query<{ version: string }>('SELECT version FROM schema_migrations ORDER BY version');
  const applied = new Set(result.rows.map(row => row.version));
  const known = new Set(migrations.map(migration => migration.version));
  const unknown = [...applied].filter(version => !known.has(version));
  if (unknown.length) throw new Error(`Database contains unknown migrations: ${unknown.join(', ')}`);
  const pending = migrations.filter(migration => !applied.has(migration.version));
  const firstPendingIndex = migrations.findIndex(migration => !applied.has(migration.version));
  if (firstPendingIndex >= 0) {
    const appliedOutOfOrder = migrations.slice(firstPendingIndex + 1).filter(migration => applied.has(migration.version));
    if (appliedOutOfOrder.length) throw new Error(`Migration history is out of order; pending ${migrations[firstPendingIndex].version} before applied ${appliedOutOfOrder.map(m => m.version).join(', ')}`);
  }
  return { applied: migrations.filter(migration => applied.has(migration.version)).map(migration => migration.version), pending: pending.map(migration => migration.version) };
}

export async function runMigrations(pool: Pool, migrations: Migration[], apply = true): Promise<Awaited<ReturnType<typeof migrationStatus>>> {
  const client = await pool.connect();
  try {
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
    await client.query('SELECT pg_advisory_lock($1)', [74192061]);
    let status = await migrationStatus(client, migrations);
    if (apply) {
      for (const version of status.pending) {
        const migration = migrations.find(item => item.version === version)!;
        await applyMigration(client, migration);
      }
      status = await migrationStatus(client, migrations);
    }
    return status;
  } finally {
    try { await client.query('SELECT pg_advisory_unlock($1)', [74192061]); }
    finally { client.release(); }
  }
}

/** Migrations that build or drop indexes CONCURRENTLY, which cannot run inside a transaction. */
const CONCURRENT_MIGRATIONS = new Set([
  '004_search_indexes',
  '008_accounts_listing_index',
  '010_transaction_filters',
  '015_transactions_listing_covering_index',
  '016_assets_index_review',
  '019_operations_index_review',
  '020_claimable_balances_listing',
  '021_ledger_audit_window_index',
]);

/** Split SQL statements while keeping semicolons inside strings and function bodies intact. */
export function splitStatements(sql: string): string[] {
  const statements: string[] = [];
  let start = 0;
  let index = 0;
  let lineComment = false;
  let blockCommentDepth = 0;
  let quote: "'" | '"' | null = null;
  let escapeString = false;
  let dollarQuote: string | null = null;

  while (index < sql.length) {
    const character = sql[index];
    const next = sql[index + 1];

    if (lineComment) {
      if (character === '\n') lineComment = false;
      index += 1;
      continue;
    }

    if (blockCommentDepth > 0) {
      if (character === '/' && next === '*') {
        blockCommentDepth += 1;
        index += 2;
      } else if (character === '*' && next === '/') {
        blockCommentDepth -= 1;
        index += 2;
      } else {
        index += 1;
      }
      continue;
    }

    if (dollarQuote) {
      if (sql.startsWith(dollarQuote, index)) {
        index += dollarQuote.length;
        dollarQuote = null;
      } else {
        index += 1;
      }
      continue;
    }

    if (quote) {
      if (character === quote && next === quote) {
        index += 2;
      } else if (character === quote) {
        quote = null;
        escapeString = false;
        index += 1;
      } else if (
        quote === "'" &&
        escapeString &&
        character === '\\'
      ) {
        index += 2;
      } else {
        index += 1;
      }
      continue;
    }

    if (character === '-' && next === '-') {
      lineComment = true;
      index += 2;
    } else if (character === '/' && next === '*') {
      blockCommentDepth = 1;
      index += 2;
    } else if (character === "'" || character === '"') {
      quote = character;
      escapeString = character === "'" &&
        /[eE]/.test(sql[index - 1] ?? '') &&
        !/[A-Za-z0-9_$]/.test(sql[index - 2] ?? '');
      index += 1;
    } else if (character === '$') {
      const delimiter = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
      if (delimiter) {
        dollarQuote = delimiter;
        index += delimiter.length;
      } else {
        index += 1;
      }
    } else if (character === ';') {
      const statement = sql.slice(start, index).replace(/^\s*--.*$/gm, '').trim();
      if (statement) statements.push(statement);
      start = index + 1;
      index += 1;
    } else {
      index += 1;
    }
  }

  if (quote || dollarQuote || blockCommentDepth > 0) {
    throw new Error('Unterminated quote or comment in SQL statements');
  }

  const finalStatement = sql.slice(start).replace(/^\s*--.*$/gm, '').trim();
  if (finalStatement) statements.push(finalStatement);
  return statements;
}

async function applyMigration(client: PoolClient, migration: Migration): Promise<void> {
  // These migrations are idempotent and write their history row last, so a
  // failure part-way is safe to re-run.
  if (CONCURRENT_MIGRATIONS.has(migration.version)) {
    for (const statement of splitStatements(migration.sql)) await client.query(statement);
    return;
  }
  await client.query('BEGIN');
  try {
    for (const statement of splitStatements(migration.sql)) await client.query(statement);
    await client.query('COMMIT');
  } catch (error) { await client.query('ROLLBACK'); throw error; }
}

/** Migrations that restructure underlying tables, partitions, or primary keys and cannot be reverted in-place. */
export const IRREVERSIBLE_MIGRATIONS = [
  '001_init',
  '006_partition_operations',
  '007_networks',
  '023_remove_placeholder_contract_interfaces',
] as const;

export function isMigrationIrreversible(version: string): boolean {
  return (IRREVERSIBLE_MIGRATIONS as readonly string[]).includes(version);
}

/**
 * Load all `.down.sql` migrations from the migrations directory.
 * Keyed by the migration version string (e.g. `002_account_event_columns`).
 */
export function loadDownMigrations(directory?: string): Map<string, Migration> {
  directory ??= [resolve(__dirname, '../../db/migrations'), resolve(__dirname, '../db/migrations')].find(existsSync)
    ?? resolve(process.cwd(), 'db/migrations');
  const downMigrations = new Map<string, Migration>();
  for (const name of readdirSync(directory)) {
    if (!/^\d+_[a-z0-9_-]+\.down\.sql$/i.test(name)) continue;
    const version = name.replace(/\.down\.sql$/, '');
    let sql = readFileSync(join(directory, name), 'utf8');
    sql = sql.replace(/^\\echo.*$/gm, '');
    downMigrations.set(version, { version, sql });
  }
  return downMigrations;
}

/**
 * Rollback a specific migration by executing its down script and removing it from `schema_migrations`.
 */
export async function rollbackMigration(pool: Pool, targetVersion: string, directory?: string): Promise<void> {
  if (isMigrationIrreversible(targetVersion)) {
    throw new Error(`Migration ${targetVersion} is irreversible and cannot be rolled back.`);
  }

  const downMigrations = loadDownMigrations(directory);
  const downMigration = downMigrations.get(targetVersion);
  if (!downMigration) {
    throw new Error(`No down migration found for version: ${targetVersion}`);
  }

  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock($1)', [74192061]);

    const result = await client.query<{ version: string }>(
      'SELECT version FROM schema_migrations WHERE version = $1',
      [targetVersion]
    );
    if (result.rows.length === 0) {
      throw new Error(`Migration ${targetVersion} has not been applied.`);
    }

    await client.query('BEGIN');
    try {
      await client.query(downMigration.sql);
      await client.query('DELETE FROM schema_migrations WHERE version = $1', [targetVersion]);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    }
  } finally {
    try {
      await client.query('SELECT pg_advisory_unlock($1)', [74192061]);
    } finally {
      client.release();
    }
  }
}

/**
 * Rollback the most recently applied migration.
 */
export async function rollbackLastMigration(pool: Pool, directory?: string): Promise<string | null> {
  const client = await pool.connect();
  let lastApplied: string | null = null;
  try {
    await client.query('SELECT pg_advisory_lock($1)', [74192061]);
    const result = await client.query<{ version: string }>(
      'SELECT version FROM schema_migrations ORDER BY applied_at DESC, version DESC LIMIT 1'
    );
    lastApplied = result.rows[0]?.version ?? null;
  } finally {
    try {
      await client.query('SELECT pg_advisory_unlock($1)', [74192061]);
    } finally {
      client.release();
    }
  }

  if (!lastApplied) return null;
  await rollbackMigration(pool, lastApplied, directory);
  return lastApplied;
}
