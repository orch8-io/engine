#!/usr/bin/env bash
set -euo pipefail

archive="${1:?usage: bash scripts/check-mobile-sqlite-host.sh <host static archive>}"
test -f "$archive"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cat > "$work/probe.c" <<'EOF'
#include <stdio.h>

/* These symbols must resolve from the mobile archive with no system -lsqlite3. */
extern const char *sqlite3_libversion(void);
extern int sqlite3_libversion_number(void);

int main(void) {
    const int minimum = 3054001;
    printf("Mobile archive SQLite %s\n", sqlite3_libversion());
    if (sqlite3_libversion_number() < minimum) {
        fputs("Mobile archive requires SQLite 3.54.1 or newer\n", stderr);
        return 1;
    }
    return 0;
}
EOF

"${CC:-cc}" "$work/probe.c" "$archive" -o "$work/probe"
"$work/probe"
