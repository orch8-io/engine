export function sqliteAdapter(connection) {
  return {
    async execAsync(sql) { connection.exec(sql); },
    async runAsync(sql, parameters) { return connection.prepare(sql).run(...parameters); },
    async getFirstAsync(sql, parameters) {
      const row = connection.prepare(sql).get(...parameters);
      return row ? { ...row } : null;
    },
  };
}
