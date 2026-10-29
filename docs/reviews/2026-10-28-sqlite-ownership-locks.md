# SQLite ownership locks and literal database paths

The Python native CI snapshot regression was intermittent. One full local suite
passed, but 100 repetitions reproduced two failures: a malformed-database error
and a stale snapshot after an external update reported one changed row and a
fresh raw read confirmed the new corrupt value. The fixture opened the same live
file using Python's SQLite library alongside the Rust extension's SQLite copy.
SQLite documents this as unsafe: the copies cannot coordinate their process-wide
file-descriptor bookkeeping, and closing one connection can clear another
connection's POSIX locks. See
[SQLite's locking explanation](https://www.sqlite.org/howtocorrupt.html#multiple_copies_of_sqlite_linked_into_the_same_application).

The fixture now performs fault injection in a separate process and checks actual
insert/update counts. The assertions remain strict: obsolete corrupt rows must
not affect a current snapshot, and corrupting the selected current diagnostic
must cause a serialization error. The production reader is unchanged.

Inspection exposed a related production defect in both native and mobile
recovery ownership. Each opened and closed the database inode before trying its
companion lock. A refused contender could therefore destroy the live owner's
SQLite locks despite never obtaining recovery authority. A deterministic test
holds a real SQLite read transaction and probes it from another process. It
failed after the refused native ownership attempt; the probe obtained an
exclusive transaction while the original reader still believed it held a lock.

Both ownership paths now share a metadata-only path resolver. It resolves an
existing database or a new filename below an existing directory, rejects hard
links and non-files, and never opens the database inode. The path is rechecked
after acquiring the stable companion lock. Native initialization opens the
canonical file whose companion it locked. Mobile connection-owned lease
retention and cancellation barriers remain in place.

Native `Storage::sqlite(Path)` also used the URI-oriented storage constructor.
A `%2F` filename reproduced failure to open the intended file. The facade now
uses a literal filename constructor; the lower-level URI constructor remains
available to existing URI callers. Cold reopen tests cover `%`, `?`, `#` and
spaces, and `:memory:` retains independent ephemeral engine semantics.

Validation passes: 12 native ownership tests, seven mobile ownership tests,
41 native snapshot/callback/deadline/preflight tests, the complete 261 storage
unit tests with PostgreSQL required, 39 facade unit tests, 20 real Python binding
tests, 100 consecutive corrected Python snapshot repetitions and 14 packed Node
binding tests. The seven mobile ownership tests are included in the 261 storage
tests. Strict all-target lints across eight consumers and standalone mobile
library compilation pass. Formatting initially caught the uncommitted delegation
publication fixture's layout; that fixture is formatted without changing its
still-failing requirements.

This repairs local database ownership and a CI fixture. Atomic server delegation
publication, durable outcome integration, the coordinator/database write floor,
legacy attribution retirement, continuing handoff/revocation and all remaining
distributed/mobile/embedded review gates remain open.
