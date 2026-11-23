import type { MediaUploadFile, MediaUploadState, MediaUploadStore } from "./mediaUpload";

import type { Orch8SqliteDatabase } from "./sqlite";

export type MediaUploadSqliteDatabase = Orch8SqliteDatabase;

const schema = `
  CREATE TABLE IF NOT EXISTS orch8_media_uploads (
    id TEXT PRIMARY KEY NOT NULL,
    length INTEGER NOT NULL CHECK (typeof(length) = 'integer' AND length BETWEEN 1 AND 9007199254740991),
    offset INTEGER NOT NULL CHECK (typeof(offset) = 'integer' AND offset BETWEEN 0 AND length),
    file_json TEXT,
    status TEXT NOT NULL CHECK (
      (status = 'pending' AND offset < length) OR
      (status = 'uploaded' AND offset = length)
    )
  )
`;

export class SqliteMediaUploadJournal implements MediaUploadStore {
  private readonly database: MediaUploadSqliteDatabase;

  private constructor(database: MediaUploadSqliteDatabase) {
    this.database = database;
  }

  static async open(database: MediaUploadSqliteDatabase): Promise<SqliteMediaUploadJournal> {
    await database.execAsync(schema);
    return new SqliteMediaUploadJournal(database);
  }

  /** Schedule one upload. Retrying with the same ID and source preserves its progress. */
  async create(id: string, length: number, file?: MediaUploadFile): Promise<MediaUploadState> {
    if (!Number.isSafeInteger(length) || length < 1) {
      throw new RangeError("Invalid media upload length");
    }
    if (file && (file.version !== 1 || file.length !== length ||
        !Number.isSafeInteger(file.chunkBytes) || file.chunkBytes < 1 || file.chunkBytes > 4 * 1024 * 1024 ||
        file.sha256.length !== Math.ceil(length / file.chunkBytes) || file.sha256.length > 65_536 ||
        !file.sha256.every((sha256) => /^[0-9a-f]{64}$/.test(sha256)) ||
        (file.fileSha256 !== undefined && !/^[0-9a-f]{64}$/.test(file.fileSha256)) ||
        !file.identity || !file.path.startsWith("/") || file.path.includes("\0"))) {
      throw new RangeError("Invalid media file manifest");
    }
    const storedFile = encodeFile(file);
    await this.database.runAsync(
      "INSERT INTO orch8_media_uploads (id, length, offset, status, file_json) VALUES (?, ?, 0, 'pending', ?) ON CONFLICT(id) DO NOTHING",
      [id, length, storedFile],
    );
    const state = await this.load(id);
    if (!state || state.length !== length) {
      throw new Error("Upload ID is already bound to a different file length");
    }
    if (encodeFile(state.file) !== storedFile) {
      throw new Error("Upload ID is already bound to a different file manifest");
    }
    return state;
  }

  async load(id: string): Promise<MediaUploadState | null> {
    const row = await this.database.getFirstAsync<MediaUploadState & { file_json: string | null }>(
      "SELECT id, length, offset, status, file_json FROM orch8_media_uploads WHERE id = ?",
      [id],
    );
    if (!row) return null;
    const { file_json, ...state } = row;
    if (file_json !== null) state.file = JSON.parse(file_json) as MediaUploadFile;
    return state;
  }

  async compareAndSet(id: string, before: MediaUploadState, after: MediaUploadState): Promise<boolean> {
    const file = encodeFile(before.file);
    if (encodeFile(after.file) !== file || before.id !== id || after.id !== id || before.length !== after.length ||
        before.status !== "pending" || !Number.isSafeInteger(after.offset) ||
        after.offset <= before.offset || after.offset > before.length ||
        after.status !== (after.offset === before.length ? "uploaded" : "pending")) {
      throw new RangeError("Invalid acknowledged media upload transition");
    }
    const result = await this.database.runAsync(
      `UPDATE orch8_media_uploads SET offset = ?, status = ?
       WHERE id = ? AND length = ? AND offset = ? AND status = ? AND file_json IS ?`,
      [after.offset, after.status, id, before.length, before.offset, before.status, file],
    );
    return result.changes === 1;
  }
}


function encodeFile(file: MediaUploadFile | undefined): string | null {
  if (!file) return null;
  const { version, path, length, identity, chunkBytes, sha256, fileSha256 } = file;
  return JSON.stringify({ version, path, length, identity, chunkBytes, sha256, fileSha256 });
}
