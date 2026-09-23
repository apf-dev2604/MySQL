import argparse
import csv
import gzip
import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple, Dict, List

import psycopg2
from psycopg2 import sql

# =============================================================================
# CHANGE VARIABLES HERE
# =============================================================================

PG_HOST = "iest-db-postgresql.cvmg4ca8uhd2.ap-southeast-1.rds.amazonaws.com"
PG_PORT = 5432
PG_USER = "egms_loader"
PG_PASSWORD = ""
PG_DATABASE = "iestdl"

# Source extract location from the MySQL extractor.
# Expected file:
#   /home/allanf/scripts/artem/out/egms_games_txn_yyyymmdd/egms_games_txn_yyyymmdd.jsonl.gz
SOURCE_BASE_DIR = Path("/home/allanf/scripts/artem/out")
FILE_PREFIX = "egms_games_txn"

# Final PostgreSQL destination is intentionally NOT defaulted.
# When --final is used, you must provide --to-schema and --to-table.

# Historical/replay table. Used after a successful --final insert.
# This table must already exist. The loader will not create it.
HISTORY_SCHEMA = "public"
HISTORY_TABLE = "temp_egms_games_txn_history"

# Transient load table. This table is truncated before every load.
TEMP_SCHEMA = "public"
TEMP_TABLE = "temp_egms_games_txn"

# Local logs.
BASE_DIR = Path("/home/allanf/scripts/artem/pg_loader")
LOG_DIR = BASE_DIR / "logs"

# Skip output directory.
# The loader writes skipped-existing-final records here.
LOAD_SKIP_OUT_DIR = BASE_DIR / "load_skip_out"

# COPY buffer flush size. This is memory control only, not a row limit.
COPY_FLUSH_ROWS = 10000
PROGRESS_EVERY_ROWS = 100000

APP_NAME = "egms_games_txn_pg_loader"


# =============================================================================
# HELPERS
# =============================================================================

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def seconds_to_hhmmss(seconds: float) -> str:
    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{APP_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )
    return log_file


def build_file_from_date(date_value: str) -> Path:
    if not (len(date_value) == 8 and date_value.isdigit()):
        raise ValueError("--date must be in yyyymmdd format. Example: 20241107")

    folder = SOURCE_BASE_DIR / f"{FILE_PREFIX}_{date_value}"
    return folder / f"{FILE_PREFIX}_{date_value}.jsonl.gz"


def get_load_date_label(args, gz_file: Path) -> str:
    """Return yyyymmdd label for output files."""
    if args.date:
        if not (len(args.date) == 8 and args.date.isdigit()):
            raise ValueError("--date must be in yyyymmdd format. Example: 20241107")
        return args.date

    # Fallback for --file usage. Try to get yyyymmdd from filename/folder.
    name_parts = [gz_file.name, gz_file.parent.name]
    for name in name_parts:
        for part in name.replace(".", "_").replace("-", "_").split("_"):
            if len(part) == 8 and part.isdigit():
                return part

    return datetime.now().strftime("%Y%m%d")


def make_skip_output_paths(args, gz_file: Path) -> Dict[str, Path]:
    date_label = get_load_date_label(args, gz_file)
    LOAD_SKIP_OUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "final_existing": LOAD_SKIP_OUT_DIR / f"{FILE_PREFIX}_{date_label}_skipped_existing_in_final.jsonl",
    }


def pg_connect(args):
    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        user=args.pg_user,
        password=args.pg_password,
        dbname=args.pg_database,
        connect_timeout=15,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load EGMS JSONL.GZ extract into PostgreSQL transient table, optionally to a selected final table."
    )

    parser.add_argument(
        "--date",
        help="Extract date in yyyymmdd format. Example: 20241107. Used to locate the source file.",
    )
    parser.add_argument(
        "--file",
        help="Optional explicit path to egms_games_txn_yyyymmdd.jsonl.gz. Overrides --date path.",
    )

    parser.add_argument("--pg-host", default=PG_HOST)
    parser.add_argument("--pg-port", type=int, default=PG_PORT)
    parser.add_argument("--pg-user", default=PG_USER)
    parser.add_argument("--pg-password", default=PG_PASSWORD)
    parser.add_argument("--pg-database", default=PG_DATABASE)

    parser.add_argument("--temp-schema", default=TEMP_SCHEMA)
    parser.add_argument("--temp-table", default=TEMP_TABLE)

    parser.add_argument(
        "--to-final", "--final",
        dest="to_final",
        action="store_true",
        help="After temp load validation PASS, insert temp rows into the selected destination table.",
    )
    parser.add_argument(
        "--to-schema", "--target-schema", "--final-schema",
        dest="target_schema",
        default=None,
        help="Destination/final schema. Required when --final is supplied.",
    )
    parser.add_argument(
        "--to-table", "--target-table", "--final-table",
        dest="target_table",
        default=None,
        help="Destination/final table. Required when --final is supplied.",
    )

    parser.add_argument(
        "--mode",
        choices=["insert-only", "upsert"],
        default="insert-only",
        help="Only used with --final. insert-only skips duplicate ids; upsert updates changed rows. Default: insert-only.",
    )

    parser.add_argument(
        "--history-schema",
        default=HISTORY_SCHEMA,
        help="Historical/replay schema. Used after a successful --final insert. Table must already exist.",
    )
    parser.add_argument(
        "--history-table",
        default=HISTORY_TABLE,
        help="Historical/replay table. Used after a successful --final insert. Table must already exist.",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="With --final, skip copying temp rows to the historical/replay table.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only connect, resolve/check table names, required columns, and privileges. Do not read file, truncate, copy, or insert.",
    )

    args = parser.parse_args()

    if not args.file and not args.date:
        parser.error("Provide --date yyyymmdd or --file /path/to/file.jsonl.gz")

    if args.to_final and (not args.target_schema or not args.target_table):
        parser.error("--final requires both --to-schema and --to-table")

    return args


def is_permission_denied_error(exc: Exception) -> bool:
    pgcode = getattr(exc, "pgcode", None)
    msg = str(exc).lower()
    return pgcode == "42501" or "permission denied" in msg or "insufficient privilege" in msg


def log_permission_context(args, stage_name: str, schema_name: str, table_name: str, exc: Exception) -> None:
    logging.error("Permission or privilege error detected.")
    logging.error("Failed stage : %s", stage_name)
    logging.error("Database     : %s", args.pg_database)
    logging.error("Schema       : %s", schema_name or "N/A")
    logging.error("Table        : %s", table_name or "N/A")
    logging.error("User         : %s", args.pg_user)
    logging.error("Host         : %s:%s", args.pg_host, args.pg_port)
    logging.error("Error        : %s", exc)


def print_permission_context(args, stage_name: str, schema_name: str, table_name: str, exc: Exception) -> None:
    print("permission_error         : YES")
    print(f"failed_stage             : {stage_name}")
    print(f"database                 : {args.pg_database}")
    print(f"schema                   : {schema_name or 'N/A'}")
    print(f"table_name               : {table_name or 'N/A'}")
    print(f"pg_user                  : {args.pg_user}")
    print(f"pg_host                  : {args.pg_host}:{args.pg_port}")
    print(f"permission_error_message : {exc}")


# =============================================================================
# VALIDATION QUERIES
# =============================================================================

def strip_pg_quotes(name: str) -> str:
    """Accept GameTx or "GameTx" from CLI and return the raw PostgreSQL name."""
    value = str(name).strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1].replace('""', '"')
    return value


def pg_qualified_display(schema_name: str, table_name: str) -> str:
    """Display PostgreSQL object names safely, preserving mixed-case table names."""
    def q(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    return f"{q(schema_name)}.{q(table_name)}"


def resolve_table_name(conn, schema_name: str, table_name: str) -> Tuple[str, str, str]:
    """
    Resolve the real PostgreSQL table name before any load action.

    Order:
      1. Exact schema + exact table match.
      2. Exact schema + case-insensitive table match, only if unique.
      3. Case-insensitive schema + case-insensitive table match, only if unique.

    This uses pg_catalog metadata only. It does not scan user data.
    """
    requested_schema = strip_pg_quotes(schema_name)
    requested_table = strip_pg_quotes(table_name)

    with conn.cursor() as cur:
        # Exact match first. This correctly finds public."GameTx" when requested table is GameTx.
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relkind IN ('r', 'p')
            LIMIT 1;
            """,
            (requested_schema, requested_table),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1], "exact"

        # Same schema, case-insensitive table match.
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND lower(c.relname) = lower(%s)
              AND c.relkind IN ('r', 'p')
            ORDER BY c.relname;
            """,
            (requested_schema, requested_table),
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], rows[0][1], "case-insensitive table match"
        if len(rows) > 1:
            options = ", ".join(pg_qualified_display(r[0], r[1]) for r in rows)
            raise RuntimeError(
                f"Ambiguous table name {pg_qualified_display(requested_schema, requested_table)}. "
                f"Multiple case-insensitive matches found: {options}"
            )

        # Case-insensitive schema + table match.
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE lower(n.nspname) = lower(%s)
              AND lower(c.relname) = lower(%s)
              AND c.relkind IN ('r', 'p')
            ORDER BY n.nspname, c.relname;
            """,
            (requested_schema, requested_table),
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], rows[0][1], "case-insensitive schema/table match"
        if len(rows) > 1:
            options = ", ".join(pg_qualified_display(r[0], r[1]) for r in rows)
            raise RuntimeError(
                f"Ambiguous table name {pg_qualified_display(requested_schema, requested_table)}. "
                f"Multiple case-insensitive schema/table matches found: {options}"
            )

        # Helpful small metadata lookup; still no user-table scan.
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE lower(n.nspname) = lower(%s)
              AND c.relkind IN ('r', 'p')
            ORDER BY c.relname
            LIMIT 20;
            """,
            (requested_schema,),
        )
        candidates = cur.fetchall()

    candidate_text = ", ".join(pg_qualified_display(r[0], r[1]) for r in candidates) or "none visible"
    raise RuntimeError(
        "Required table does not exist or is not visible to this user. "
        f"Requested: {pg_qualified_display(requested_schema, requested_table)}. "
        f"Visible tables in requested schema sample: {candidate_text}"
    )


def required_columns_for_purpose(purpose: str, mode: str) -> Dict[str, str]:
    if purpose == "temp":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    if purpose == "final":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    if purpose == "history":
        return {"id": "text-compatible", "data": "json/jsonb", "game_dt": "timestamp/timestamptz"}
    return {}


def assert_required_columns(conn, schema_name: str, table_name: str, purpose: str, mode: str) -> None:
    required = required_columns_for_purpose(purpose, mode)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped;
            """,
            (schema_name, table_name),
        )
        cols = {name: dtype for name, dtype in cur.fetchall()}

    missing = [col for col in required if col not in cols]
    if missing:
        raise RuntimeError(
            f"Required column(s) missing on {pg_qualified_display(schema_name, table_name)}: {', '.join(missing)}"
        )


def assert_table_privileges(conn, pg_user: str, schema_name: str, table_name: str, required_privileges: List[str]) -> None:
    qname = pg_qualified_display(schema_name, table_name)
    with conn.cursor() as cur:
        failed = []
        for priv in required_privileges:
            cur.execute("SELECT has_table_privilege(%s, %s, %s);", (pg_user, qname, priv))
            if not bool(cur.fetchone()[0]):
                failed.append(priv)
    if failed:
        raise RuntimeError(
            f"Missing privilege(s) on {qname} for user {pg_user}: {', '.join(failed)}"
        )


def resolve_and_validate_table(conn, args, purpose: str, schema_attr: str, table_attr: str) -> Tuple[str, str]:
    requested_schema = getattr(args, schema_attr)
    requested_table = getattr(args, table_attr)
    resolved_schema, resolved_table, resolution = resolve_table_name(conn, requested_schema, requested_table)

    logging.info(
        "Resolved %s table: requested=%s resolved=%s resolution=%s",
        purpose,
        pg_qualified_display(strip_pg_quotes(requested_schema), strip_pg_quotes(requested_table)),
        pg_qualified_display(resolved_schema, resolved_table),
        resolution,
    )

    # Store resolved exact names back into args so all later SQL uses the correct quoted identifier.
    setattr(args, schema_attr, resolved_schema)
    setattr(args, table_attr, resolved_table)

    assert_required_columns(conn, resolved_schema, resolved_table, purpose, args.mode)

    if purpose == "temp":
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, ["SELECT", "INSERT", "TRUNCATE"])
    elif purpose == "final":
        required = ["SELECT", "INSERT"]
        if args.mode == "upsert":
            required.append("UPDATE")
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, required)
    elif purpose == "history":
        assert_table_privileges(conn, args.pg_user, resolved_schema, resolved_table, ["INSERT"])

    return resolved_schema, resolved_table


def truncate_temp(conn, args) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {schema}.{table};").format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
    conn.commit()


def get_temp_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {schema}.{table};").format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
        return int(cur.fetchone()[0])


def get_temp_duplicate_id_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
            SELECT COUNT(*)
            FROM (
                SELECT id
                FROM {schema}.{table}
                GROUP BY id
                HAVING COUNT(*) > 1
            ) d;
            """).format(
                schema=sql.Identifier(args.temp_schema),
                table=sql.Identifier(args.temp_table),
            )
        )
        return int(cur.fetchone()[0])


def get_existing_in_final_count(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
            SELECT COUNT(*)
            FROM {temp_schema}.{temp_table} t
            JOIN {target_schema}.{target_table} f
              ON f.id = t.id;
            """).format(
                temp_schema=sql.Identifier(args.temp_schema),
                temp_table=sql.Identifier(args.temp_table),
                target_schema=sql.Identifier(args.target_schema),
                target_table=sql.Identifier(args.target_table),
            )
        )
        return int(cur.fetchone()[0])


def write_existing_in_final_skips(conn, args, output_file: Path) -> int:
    """Write id, game_dt, and loaded_time for rows from temp that already exist in final."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    query = sql.SQL("""
        SELECT
            t.id,
            t.game_dt,
            now() AS loaded_time
        FROM {temp_schema}.{temp_table} t
        JOIN {target_schema}.{target_table} f
          ON f.id = t.id
        ORDER BY t.game_dt, t.id;
    """).format(
        temp_schema=sql.Identifier(args.temp_schema),
        temp_table=sql.Identifier(args.temp_table),
        target_schema=sql.Identifier(args.target_schema),
        target_table=sql.Identifier(args.target_table),
    )

    # Named cursor streams result rows from PostgreSQL instead of holding all rows in memory.
    cursor_name = f"skip_existing_{int(time.time())}"
    with conn.cursor(name=cursor_name) as cur, output_file.open("w", encoding="utf-8") as out:
        cur.itersize = 10000
        cur.execute(query)
        for row in cur:
            count += 1
            row_id, game_dt, loaded_time = row
            out.write(json.dumps({
                "id": row_id,
                "game_dt": game_dt,
                "loaded_time": loaded_time,
            }, ensure_ascii=False, default=json_default))
            out.write("\n")

    if count == 0:
        try:
            output_file.unlink()
        except FileNotFoundError:
            pass

    return count

# =============================================================================
# FILE READ + COPY
# =============================================================================

def stable_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def count_jsonl_rows(gz_file: Path) -> int:
    count = 0
    with gzip.open(str(gz_file), "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
                if count % PROGRESS_EVERY_ROWS == 0:
                    logging.info("Progress : file_line_count=%s", count)
    return count


def copy_jsonl_to_temp(conn, args, gz_file: Path) -> int:
    """
    Stream JSONL.GZ into PostgreSQL transient table using COPY.
    The source JSONL row must have:
        id      = TransactionID
        data    = JSON payload
        game_dt = GameDate
    """
    copied_count = 0
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    copy_sql = sql.SQL("""
        COPY {schema}.{table} (id, data, game_dt)
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '"', ESCAPE '"')
    """).format(
        schema=sql.Identifier(args.temp_schema),
        table=sql.Identifier(args.temp_table),
    )

    def flush_buffer(cur):
        nonlocal buffer
        buffer.seek(0)
        cur.copy_expert(copy_sql.as_string(conn), buffer)
        buffer.close()
        buffer = io.StringIO()
        return csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    with conn.cursor() as cur:
        with gzip.open(str(gz_file), "rt", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue

                try:
                    obj = json.loads(line)
                    row_id = str(obj["id"])
                    data_obj = obj["data"]
                    game_dt = str(obj["game_dt"])
                    data_text = stable_json_text(data_obj)
                except Exception as exc:
                    raise ValueError(f"Invalid JSONL row at line {line_no}: {exc}")

                writer.writerow([row_id, data_text, game_dt])
                copied_count += 1

                if copied_count % COPY_FLUSH_ROWS == 0:
                    writer = flush_buffer(cur)

                if copied_count % PROGRESS_EVERY_ROWS == 0:
                    logging.info("Progress : copied_to_temp_count=%s", copied_count)

            if buffer.tell() > 0:
                flush_buffer(cur)

    conn.commit()
    return copied_count


# =============================================================================
# FINAL INSERT + HISTORY COPY
# =============================================================================

def insert_to_final(conn, args) -> Tuple[int, int, int]:
    """
    Returns:
        inserted_count, updated_count, skipped_count

    insert-only:
        duplicate id = do nothing
        missing id   = insert

    upsert:
        duplicate id with changed data/game_dt = update
    """
    with conn.cursor() as cur:
        if args.mode == "insert-only":
            cur.execute(
                sql.SQL("""
                WITH src AS (
                    SELECT t.id, t.data, t.game_dt
                    FROM {temp_schema}.{temp_table} t
                ), ins AS (
                    INSERT INTO {target_schema}.{target_table} (id, data, game_dt)
                    SELECT id, data, game_dt
                    FROM src
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                )
                SELECT
                    (SELECT COUNT(*) FROM ins) AS inserted_count,
                    0 AS updated_count,
                    (SELECT COUNT(*) FROM src) - (SELECT COUNT(*) FROM ins) AS skipped_count;
                """).format(
                    temp_schema=sql.Identifier(args.temp_schema),
                    temp_table=sql.Identifier(args.temp_table),
                    target_schema=sql.Identifier(args.target_schema),
                    target_table=sql.Identifier(args.target_table),
                )
            )
        else:
            cur.execute(
                sql.SQL("""
                WITH src AS (
                    SELECT t.id, t.data, t.game_dt
                    FROM {temp_schema}.{temp_table} t
                ), before_match AS (
                    SELECT
                        s.id,
                        CASE
                            WHEN f.id IS NULL THEN 'insert'
                            WHEN f.data IS DISTINCT FROM s.data
                              OR f.game_dt IS DISTINCT FROM s.game_dt THEN 'update'
                            ELSE 'skip'
                        END AS action_type
                    FROM src s
                    LEFT JOIN {target_schema}.{target_table} f
                      ON f.id = s.id
                ), upserted AS (
                    INSERT INTO {target_schema}.{target_table} (id, data, game_dt)
                    SELECT id, data, game_dt
                    FROM src
                    ON CONFLICT (id)
                    DO UPDATE SET
                        data = EXCLUDED.data,
                        game_dt = EXCLUDED.game_dt
                    WHERE {target_schema}.{target_table}.data IS DISTINCT FROM EXCLUDED.data
                       OR {target_schema}.{target_table}.game_dt IS DISTINCT FROM EXCLUDED.game_dt
                    RETURNING id
                )
                SELECT
                    COUNT(*) FILTER (WHERE action_type = 'insert') AS inserted_count,
                    COUNT(*) FILTER (WHERE action_type = 'update') AS updated_count,
                    COUNT(*) FILTER (WHERE action_type = 'skip') AS skipped_count
                FROM before_match;
                """).format(
                    temp_schema=sql.Identifier(args.temp_schema),
                    temp_table=sql.Identifier(args.temp_table),
                    target_schema=sql.Identifier(args.target_schema),
                    target_table=sql.Identifier(args.target_table),
                )
            )

        result = cur.fetchone()

    conn.commit()
    return int(result[0]), int(result[1]), int(result[2])


def copy_temp_to_history(conn, args) -> int:
    """
    Fast set-based, idempotent copy from temp to historical/replay table.
    The history table must already exist and must have at least:
        id TEXT with PRIMARY KEY or UNIQUE constraint
        data JSONB
        game_dt TIMESTAMPTZ

    Existing history IDs are preserved using ON CONFLICT (id) DO NOTHING.
    """
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
            WITH ins AS (
                INSERT INTO {history_schema}.{history_table} (id, data, game_dt)
                SELECT id, data, game_dt
                FROM {temp_schema}.{temp_table}
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            )
            SELECT COUNT(*) FROM ins;
            """).format(
                history_schema=sql.Identifier(args.history_schema),
                history_table=sql.Identifier(args.history_table),
                temp_schema=sql.Identifier(args.temp_schema),
                temp_table=sql.Identifier(args.temp_table),
            )
        )
        copied_rows = cur.fetchone()[0]
    conn.commit()
    return int(copied_rows)


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    args = parse_args()
    log_file = setup_logging(LOG_DIR)

    if args.file:
        gz_file = Path(args.file)
    else:
        gz_file = build_file_from_date(args.date)

    if not gz_file.exists():
        logging.error("Input file not found: %s", gz_file)
        return 1

    started_at = time.time()
    started_text = now_text()

    final_destination_table = pg_qualified_display(args.target_schema, args.target_table) if args.to_final else "not loaded; temp validation only"
    history_destination_table = pg_qualified_display(args.history_schema, args.history_table) if args.to_final and not args.skip_history else "not copied"
    current_stage = "initializing"
    current_schema = ""
    current_table = ""

    logging.info("Loader started")
    logging.info("Started at PHT : %s", started_text)
    logging.info("Remote PG      : %s:%s", args.pg_host, args.pg_port)
    logging.info("Database       : %s", args.pg_database)
    logging.info("PG user        : %s", args.pg_user)
    logging.info("Source file    : %s", gz_file)
    logging.info("Temp table     : %s.%s", args.temp_schema, args.temp_table)
    logging.info("Final table    : %s", final_destination_table)
    logging.info("History table  : %s", history_destination_table)
    logging.info("To final       : %s", args.to_final)
    logging.info("Mode           : %s", args.mode if args.to_final else "temp-only dry-run")
    logging.info("Log file       : %s", log_file)
    logging.info("Flush size     : %s", COPY_FLUSH_ROWS)
    logging.info("Progress every : %s rows", PROGRESS_EVERY_ROWS)
    logging.info("Check only     : %s", args.check_only)
    logging.info("Load skip out  : %s", LOAD_SKIP_OUT_DIR)

    conn = None
    file_count = 0
    copied_count = 0
    temp_count = 0
    duplicate_id_count = 0
    existing_in_final_count = 0
    expected_new_count = 0
    final_inserted_count = 0
    final_updated_count = 0
    final_skipped_count = 0
    final_duration = 0.0
    history_copied_count = 0
    history_duration = 0.0
    migration_status = "NOT READY"
    final_existing_skip_file = "not generated"

    skip_output_paths = make_skip_output_paths(args, gz_file)
    final_existing_skip_file = str(skip_output_paths["final_existing"])

    try:
        current_stage = "connect to PostgreSQL"
        conn = pg_connect(args)

        current_stage = "resolve and validate temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        resolve_and_validate_table(conn, args, "temp", "temp_schema", "temp_table")

        if args.to_final:
            current_stage = "resolve and validate final table"
            current_schema = args.target_schema
            current_table = args.target_table
            resolve_and_validate_table(conn, args, "final", "target_schema", "target_table")

            if not args.skip_history:
                current_stage = "resolve and validate history table"
                current_schema = args.history_schema
                current_table = args.history_table
                resolve_and_validate_table(conn, args, "history", "history_schema", "history_table")

        final_destination_table = pg_qualified_display(args.target_schema, args.target_table) if args.to_final else "not loaded; temp validation only"
        history_destination_table = pg_qualified_display(args.history_schema, args.history_table) if args.to_final and not args.skip_history else "not copied"

        if args.check_only:
            completed_at = time.time()
            ended_text = now_text()
            duration = completed_at - started_at
            print("\nTABLE CHECK SUMMARY")
            print("===================")
            print("status                    : PASS")
            print("check_only                : YES")
            print(f"started_at_pht            : {started_text}")
            print(f"completed_at_pht          : {ended_text}")
            print(f"duration_seconds          : {duration:.2f}")
            print(f"remote_pg                 : {args.pg_host}:{args.pg_port}")
            print(f"database                  : {args.pg_database}")
            print(f"temp_table_resolved       : {pg_qualified_display(args.temp_schema, args.temp_table)}")
            print(f"final_table_resolved      : {final_destination_table}")
            print(f"history_table_resolved    : {history_destination_table}")
            print("next_step                 : run without --check-only to load temp/dry-run or add --final to migrate")
            return 0

        current_stage = "count JSONL rows"
        current_schema = ""
        current_table = ""
        logging.info("Counting JSONL rows in file...")
        count_start = time.time()
        file_count = count_jsonl_rows(gz_file)
        count_duration = time.time() - count_start
        logging.info("File row count : %s", file_count)
        logging.info("File count duration seconds: %.2f", count_duration)

        current_stage = "truncate temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        logging.info("Truncating transient table...")
        truncate_temp(conn, args)
        logging.info("Transient table truncated: %s.%s", args.temp_schema, args.temp_table)

        current_stage = "copy file to temp table"
        current_schema = args.temp_schema
        current_table = args.temp_table
        logging.info("Copying JSONL.GZ into transient table...")
        copy_start = time.time()
        copied_count = copy_jsonl_to_temp(conn, args, gz_file)
        copy_duration = time.time() - copy_start
        logging.info("Copied count from file: %s", copied_count)
        logging.info("Copy duration seconds: %.2f", copy_duration)

        current_stage = "validate temp table counts"
        current_schema = args.temp_schema
        current_table = args.temp_table
        temp_count = get_temp_count(conn, args)
        duplicate_id_count = get_temp_duplicate_id_count(conn, args)
        logging.info("Transient table row count: %s", temp_count)
        logging.info("Duplicate id count in transient table: %s", duplicate_id_count)

        if not (file_count == copied_count == temp_count):
            raise RuntimeError(
                f"FAIL: Count mismatch. file_count={file_count}, copied_count={copied_count}, temp_count={temp_count}. Final insert aborted."
            )

        if duplicate_id_count > 0:
            raise RuntimeError(
                f"FAIL: Duplicate id detected in transient table: duplicate_id_count={duplicate_id_count}. Final insert aborted."
            )

        migration_status = "GOOD FOR MIGRATION"
        logging.info("PASS: file_count, copied_count, and transient table count match.")
        logging.info("GOOD FOR MIGRATION: temp table is loaded and validated.")

        if args.to_final:
            current_stage = "pre-check existing ids in final table"
            current_schema = args.target_schema
            current_table = args.target_table
            existing_in_final_count = write_existing_in_final_skips(
                conn, args, skip_output_paths["final_existing"]
            )
            expected_new_count = temp_count - existing_in_final_count
            logging.info("Existing ids in final before insert: %s", existing_in_final_count)
            logging.info("Expected new rows to insert: %s", expected_new_count)
            if existing_in_final_count > 0:
                logging.info("Skipped-existing-final records written to: %s", skip_output_paths["final_existing"])

            current_stage = "insert/upsert to final table"
            logging.info("--final supplied. Loading transient rows to final table...")
            final_start = time.time()
            final_inserted_count, final_updated_count, final_skipped_count = insert_to_final(conn, args)
            final_duration = time.time() - final_start
            logging.info("Final load completed")
            logging.info("Final inserted count: %s", final_inserted_count)
            logging.info("Final updated count: %s", final_updated_count)
            logging.info("Final skipped count: %s", final_skipped_count)
            logging.info("Final load duration seconds: %.2f", final_duration)

            migration_status = f"LOADED IN {final_destination_table}"

            if not args.skip_history:
                current_stage = "copy temp rows to history table"
                current_schema = args.history_schema
                current_table = args.history_table
                logging.info("Copying all temp rows to history/replay table: %s.%s", args.history_schema, args.history_table)
                history_start = time.time()
                history_copied_count = copy_temp_to_history(conn, args)
                history_duration = time.time() - history_start
                logging.info("History copy completed. copied_rows=%s duration_seconds=%.2f", history_copied_count, history_duration)
        else:
            logging.info("--final not supplied. Dry-run stopped after transient table validation PASS.")

        completed_at = time.time()
        ended_text = now_text()
        duration = completed_at - started_at

        logging.info("Completed at PHT: %s", ended_text)
        logging.info("Duration        : %.3f minutes (%s)", duration / 60.0, seconds_to_hhmmss(duration))
        logging.info("Load completed successfully. temp_count=%s to_final=%s", temp_count, args.to_final)

        print("\nLOAD SUMMARY")
        print("============")
        print("status                    : PASS")
        print(f"migration_status          : {migration_status}")
        print(f"started_at_pht            : {started_text}")
        print(f"completed_at_pht          : {ended_text}")
        print(f"duration_seconds          : {duration:.2f}")
        print(f"duration_minutes          : {duration / 60.0:.3f}")
        print(f"duration_hhmmss           : {seconds_to_hhmmss(duration)}")
        print(f"remote_pg                 : {args.pg_host}:{args.pg_port}")
        print(f"database                  : {args.pg_database}")
        print(f"source_file               : {gz_file}")
        print(f"temp_table                : {args.temp_schema}.{args.temp_table}")
        print(f"final_table               : {final_destination_table}")
        print(f"history_table             : {history_destination_table}")
        print(f"file_jsonl_line_count     : {file_count}")
        print(f"copied_to_temp_count      : {copied_count}")
        print(f"temp_table_count          : {temp_count}")
        print(f"duplicate_id_count        : {duplicate_id_count}")
        print(f"to_final                  : {args.to_final}")
        print(f"mode                      : {args.mode if args.to_final else 'temp-only dry-run'}")

        if args.to_final:
            print(f"existing_in_final_before  : {existing_in_final_count}")
            print(f"expected_new_rows         : {expected_new_count}")
            print(f"final_existing_skip_file  : {final_existing_skip_file if existing_in_final_count > 0 else 'none'}")
            print(f"final_inserted_count      : {final_inserted_count}")
            print(f"final_updated_count       : {final_updated_count}")
            print(f"final_skipped_count       : {final_skipped_count}")
            print(f"final_duration_seconds    : {final_duration:.2f}")
            if not args.skip_history:
                print(f"history_copied_count      : {history_copied_count}")
                print(f"history_duration_seconds  : {history_duration:.2f}")
        else:
            print("final_load_status         : not loaded; temp validation only")
            print("next_step                 : rerun with --final --to-schema <schema> --to-table <table>")

        return 0

    except Exception as exc:
        logging.exception("Loader failed: %s", exc)
        if is_permission_denied_error(exc):
            log_permission_context(args, current_stage, current_schema, current_table, exc)
        if conn:
            conn.rollback()

        completed_at = time.time()
        ended_text = now_text()
        duration = completed_at - started_at

        print("\nLOAD SUMMARY")
        print("============")
        print("status                    : FAIL")
        print(f"started_at_pht            : {started_text}")
        print(f"completed_at_pht          : {ended_text}")
        print(f"duration_seconds          : {duration:.2f}")
        print(f"duration_minutes          : {duration / 60.0:.3f}")
        print(f"duration_hhmmss           : {seconds_to_hhmmss(duration)}")
        print(f"remote_pg                 : {args.pg_host}:{args.pg_port}")
        print(f"database                  : {args.pg_database}")
        print(f"source_file               : {gz_file}")
        print(f"temp_table                : {args.temp_schema}.{args.temp_table}")
        print(f"final_table               : {final_destination_table}")
        print(f"history_table             : {history_destination_table}")
        print(f"file_jsonl_line_count     : {file_count}")
        print(f"copied_to_temp_count      : {copied_count}")
        print(f"temp_table_count          : {temp_count}")
        print(f"final_existing_skip_file  : {final_existing_skip_file if existing_in_final_count > 0 else 'none'}")
        print(f"failed_stage              : {current_stage}")
        print(f"error                     : {exc}")
        if is_permission_denied_error(exc):
            print_permission_context(args, current_stage, current_schema, current_table, exc)
        return 1

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
