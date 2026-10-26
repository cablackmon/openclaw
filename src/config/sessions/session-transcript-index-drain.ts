import { executeSqliteQuerySync, getNodeSqliteKysely } from "../../infra/kysely-sync.js";
import type { EngineConnection } from "../../infra/sql-connection.js";
import {
  sqliteSessionIdWriteScope,
  withSqliteDatabaseWriteScope,
} from "../../infra/sqlite-database-admission.js";
import type { DB } from "../../state/openclaw-agent-db.generated.js";
import { deleteSessionTranscriptFtsRowsWithinBudgetInTransaction } from "./session-transcript-fts.js";
import { markSessionTranscriptIndexDirtyInTransaction } from "./session-transcript-index.js";

type TranscriptIndexDrainDatabase = Pick<DB, "session_transcript_active_events">;

/**
 * Drains one bounded batch of a doomed session's derived projection ahead of its deletion
 * transaction. Dirty state lands first, so readers fall back to canonical rows and a refused
 * deletion is rebuilt by reconcile; canonical transcript rows are never touched here.
 */
export function drainSessionTranscriptIndexInTransaction(
  db: EngineConnection,
  sessionId: string,
  budget: { maxRows: number; maxBytes: number },
): { deletedRows: number; exhausted: boolean } {
  markSessionTranscriptIndexDirtyInTransaction(db, sessionId);
  const fts = deleteSessionTranscriptFtsRowsWithinBudgetInTransaction(db, sessionId, budget);
  const activeBudget = budget.maxRows - fts.deletedRows;
  if (!fts.exhausted || activeBudget <= 0) {
    return { deletedRows: fts.deletedRows, exhausted: false };
  }
  const kysely = getNodeSqliteKysely<TranscriptIndexDrainDatabase>(db);
  const active = executeSqliteQuerySync(
    db,
    kysely
      .selectFrom("session_transcript_active_events")
      .select("active_position")
      .where("session_id", "=", sessionId)
      .orderBy("active_position")
      .limit(activeBudget + 1),
  ).rows.map((row) => row.active_position);
  const deleted = active.slice(0, activeBudget);
  if (deleted.length > 0) {
    withSqliteDatabaseWriteScope(db, [sqliteSessionIdWriteScope(sessionId)], () =>
      executeSqliteQuerySync(
        db,
        kysely
          .deleteFrom("session_transcript_active_events")
          .where("session_id", "=", sessionId)
          .where("active_position", "in", deleted),
      ),
    );
  }
  return {
    deletedRows: fts.deletedRows + deleted.length,
    exhausted: deleted.length === active.length,
  };
}
