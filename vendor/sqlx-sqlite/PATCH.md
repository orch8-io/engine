# SQLx SQLite compatibility patch

Source: published `sqlx-sqlite` 0.9.0, MIT OR Apache-2.0.
Both upstream license files are retained. All driver source files are unchanged.

The only manifest change narrows `libsqlite3-sys` to `>=0.39.1, <0.40.0`.
This selects SQLite 3.54.1, fixing [RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459.html).
The released SQLx manifest excludes these fixed versions; [upstream now permits 0.39](https://github.com/launchbadge/sqlx/blob/main/sqlx-sqlite/Cargo.toml).

Remove this patch and the root and standalone workspace overrides when a released
SQLx driver permits patched SQLite. Keep the SQLite lockfile version at 0.39.1 or newer.
