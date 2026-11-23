type SqliteParameters = (string | number | null)[];

/** A dedicated SQLite database handle, such as one opened with expo-sqlite. */
export interface Orch8SqliteDatabase {
  execAsync(sql: string): Promise<void>;
  runAsync(sql: string, parameters: SqliteParameters): Promise<{ changes: number }>;
  getFirstAsync<T>(sql: string, parameters: SqliteParameters): Promise<T | null>;
}
