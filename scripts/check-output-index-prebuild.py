#!/usr/bin/env python3
"""Exercise output-index rollout in a fresh, automatically removed PG schema."""
import os
import subprocess
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL and the psql client are required")
    schema = "orch8_prebuild_" + uuid.uuid4().hex
    script = root / "scripts/sql/prebuild-current-output-indexes.psql"

    def require(condition, message):
        if not condition:
            raise RuntimeError(message)

    def run(sql=None, file=None, ok=True):
        args = ["psql", url, "-X", "--set=ON_ERROR_STOP=1", "--set=VERBOSITY=verbose", "-qAt",
                "--command", f"SET search_path TO {schema}, pg_catalog"]
        args += ["--file", str(file)] if file else ["--command", sql]
        result = subprocess.run(args, text=True, capture_output=True, timeout=120)
        if ok and result.returncode:
            raise RuntimeError(result.stderr[-2500:])
        return result

    def scalar(sql, expected):
        require(run(sql).stdout.strip() == expected, "Unexpected prebuild state: " + sql)

    created = False
    try:
        run(f"CREATE SCHEMA {schema}")
        created = True
        ddl = (root / "migrations/074_partition_high_volume_tables.sql").read_text()
        start = ddl.index("CREATE TABLE block_outputs (")
        end = ddl.index("$partition$;", start) + len("$partition$;")
        run("CREATE TABLE task_instances(id UUID PRIMARY KEY);\n" + ddl[start:end] + """
INSERT INTO task_instances VALUES (gen_random_uuid());
INSERT INTO block_outputs(id,instance_id,block_id,output)
SELECT gen_random_uuid(),id,'same','{}'::jsonb
FROM task_instances CROSS JOIN generate_series(1,2);
""")
        rejected = run("CREATE INDEX CONCURRENTLY invalid_parent_probe ON block_outputs(block_id)", ok=False)
        require(rejected.returncode and "0A000" in rejected.stderr,
                "Expected parent-concurrent rejection (SQLSTATE 0A000)")
        run(file=script)
        reserved = (f"c.relnamespace='{schema}'::regnamespace AND "
                    "substr(c.relname::text,1,10) IN ('orch8_boc_','orch8_bod_')")
        scalar("SELECT count(*) FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid "
               "WHERE " + reserved + " AND i.indisvalid", "32")
        run("CREATE TABLE before_index_files AS SELECT c.oid,pg_relation_filenode(c.oid) AS node "
            "FROM pg_class c WHERE " + reserved)
        run("BEGIN;\n" + (root / "migrations/125_current_output_projection.sql").read_text()
            + "\n" + (root / "migrations/126_current_diagnostic_outputs.sql").read_text() + "\nCOMMIT;")
        parents = "('idx_block_outputs_current'::regclass,'idx_block_outputs_diagnostics'::regclass)"
        scalar("SELECT count(*),bool_and(indisvalid) FROM pg_index WHERE indexrelid IN " + parents, "2|t")
        scalar("SELECT count(*) FROM pg_inherits h JOIN before_index_files b ON b.oid=h.inhrelid "
               "WHERE h.inhparent IN " + parents, "32")
        scalar("SELECT count(*) FROM before_index_files WHERE node=pg_relation_filenode(oid)", "32")
        run(file=script)
        scalar("SELECT count(*) FROM pg_class c WHERE " + reserved, "32")
        leaf = run("SELECT tableoid::regclass::text FROM block_outputs LIMIT 1").stdout.strip()
        failed = run(f"CREATE UNIQUE INDEX CONCURRENTLY orch8_boc_fault ON {leaf}(block_id)", ok=False)
        require(failed.returncode and "23505" in failed.stderr,
                "Expected a failed concurrent leaf build (SQLSTATE 23505)")
        refused = run(file=script, ok=False)
        require(refused.returncode and "P0001" in refused.stderr
                and "Repair invalid output prebuild indexes" in refused.stderr,
                "Invalid concurrent build must be refused before migration")
        run("DROP INDEX CONCURRENTLY orch8_boc_fault")
        run(file=script)
        scalar("SELECT count(*) FROM block_outputs", "2")
        print("PASS: 32 concurrent leaf indexes reused without rebuild; repeat and invalid-index recovery verified")
    finally:
        if created:
            run(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


if __name__ == "__main__":
    main()
