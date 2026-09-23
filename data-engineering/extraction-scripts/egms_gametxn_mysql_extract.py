#!/usr/bin/env python3
"""
Safe EGMS Game Transactions extractor for Python 3.9.

Purpose:
- Extract all rows in a GameDate/UpdateDateTime window from a remote MariaDB/MySQL server.
- Use date-window SQL only; no SQL pagination condition and no SQL LIMIT by default.
- Use a streaming cursor and fetchmany() so Python does not load all rows in memory.
- Use small internal fetch batches, optional sleep, and low OS priority to reduce host impact.
- Write PostgreSQL-ready JSONL.GZ output with shape: id, data, game_dt.
- Treat source MariaDB/MySQL datetime values as UTC/system time and write UTC ISO milliseconds with Z.
- Omit Idx and write data JSON keys in the approved PostgreSQL payload order.
- Compute:
    JACKPOT_PAYOUT       = JW1 + JW2 + JW3 + JW4 + JW5
    JACKPOT_CONTRIBUTION = PC1 + PC2 + PC3 + PC4 + PC5
- Validate source database count against generated JSONL row count before PASS.
- Create a manifest with row count, checksum, start/end time, and duration.
- Optional local or SFTP handoff.

Recommended MySQL/MariaDB index:
    CREATE INDEX idx_egms_gamedate ON Artemis.EGMS_Game_Trans (GameDate);

For change-aware extracts:
    CREATE INDEX idx_egms_updatedatetime ON Artemis.EGMS_Game_Trans (UpdateDateTime);

Run example:
    python3.9 egms_games_txn_mysql_extract_utc_source.py \
      --db Artemis \
      --table EGMS_Game_Trans \
      --pwd 'CHANGE_ME' \
      --from '2025-11-28 06:00:00' \
      --to '2025-11-29 06:00:00' \
      --date-column GameDate \
      --dest local
"""

import argparse
import fcntl
import gzip
import hashlib
import json
import logging
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import pymysql
from pymysql.cursors import SSDictCursor

try:
    import paramiko
except ImportError:
    paramiko = None


# =============================================================================
# CHANGE VARIABLES HERE
# =============================================================================

# Remote MariaDB/MySQL connection settings.
MYSQL_HOST = "103.253.145.33"          # remote MySQL/MariaDB server IP/DNS
MYSQL_PORT = 3306
MYSQL_USER = "AllanFaylona"           # use a read-only MySQL/MariaDB user

DEFAULT_DB = "Artemis"
DEFAULT_TABLE = "EGMS_Game_Trans"

# PostgreSQL target mapping:
# target id      = TransactionID
# target data    = JSON payload created from source columns
# target game_dt = GameDate
OUTPUT_ID_COLUMN = "TransactionID"
GAME_DT_OUTPUT_COLUMN = "GameDate"
DEFAULT_DATE_COLUMN = "GameDate"

# File naming.
FILE_PREFIX = "egms_games_txn"

# Local runtime timestamps still use PHT for log readability only.
PHT_OFFSET = timezone(timedelta(hours=8))

# Source business/filter time is UTC/system time in MariaDB/MySQL.
SOURCE_TIMEZONE = timezone.utc

# Local directories.
BASE_DIR = Path("/home/allanf/scripts/artem")
OUTPUT_DIR = BASE_DIR / "out"
WORK_DIR = BASE_DIR / "work"
LOG_DIR = BASE_DIR / "logs"
LOCK_FILE = BASE_DIR / "extract_egms_gamestx.lock"

# Destination.
DEFAULT_DEST = "local"  # local or sftp

# SFTP settings. Used only when --dest sftp.
SFTP_HOST = "sftp.example.com"
SFTP_PORT = 22
SFTP_USER = "etluser"
SFTP_PRIVATE_KEY = Path("/opt/mysql_extract/keys/sftp_id_rsa")
SFTP_REMOTE_DIR = "/incoming/mysql_extract"

# Safety controls.
# FETCH_SIZE controls Python memory usage only. It is NOT a total row limit.
FETCH_SIZE = 10000
SLEEP_SECONDS_BETWEEN_FETCHES = 0.20
PROGRESS_LOG_EVERY_ROWS = 100000

# Optional safety cap for testing. Use 0 for unlimited/full extract.
MAX_ROWS_TO_EXTRACT = 0

# Lower OS CPU priority where supported. Higher number means lower priority.
ENABLE_OS_NICE = True
OS_NICE_INCREMENT = 10

# Disable COUNT(*) by default to avoid extra load on large tables.
ENABLE_SOURCE_COUNT = False

# Retention.
LOCAL_RETENTION_DAYS = 14
LOG_RETENTION_DAYS = 30

APP_NAME = "extract_egms_gamestx_safe"

# Source columns to SELECT from MariaDB/MySQL.
# Idx is intentionally omitted from the extract payload.
# Computed fields are added in Python and then ordered using PAYLOAD_COLUMNS.
SOURCE_SELECT_COLUMNS = [
    "JW1",
    "JW2",
    "JW3",
    "JW4",
    "JW5",
    "PC1",
    "PC2",
    "PC3",
    "PC4",
    "PC5",
    "Outlet",
    "GameDate",
    "GameName",
    "SessionID",
    "TotalWins",
    "TotalStakes",
    "GameProvider",
    "PlayerAccount",
    "TransactionID",
    "SEED_MONEY_WON",
    "UpdateDateTime",
    "PROGRESSIVE_CONTRIBUTION_PAID",
    "SEED_MONEY_JACKPOT_WON_OVER_1000",
]

# Exact JSONB data payload order required for PostgreSQL load.
PAYLOAD_COLUMNS = [
    "JW1",
    "JW2",
    "JW3",
    "JW4",
    "JW5",
    "PC1",
    "PC2",
    "PC3",
    "PC4",
    "PC5",
    "Outlet",
    "GameDate",
    "GameName",
    "SessionID",
    "TotalWins",
    "TotalStakes",
    "GameProvider",
    "PlayerAccount",
    "TransactionID",
    "JACKPOT_PAYOUT",
    "SEED_MONEY_WON",
    "UpdateDateTime",
    "JACKPOT_CONTRIBUTION",
    "PROGRESSIVE_CONTRIBUTION_PAID",
    "SEED_MONEY_JACKPOT_WON_OVER_1000",
]

# Backward-compatible alias used by some validation logic.
SOURCE_COLUMNS = SOURCE_SELECT_COLUMNS


# =============================================================================
# CONFIG
# =============================================================================

@dataclass
class Config:
    db: str
    table: str
    pwd: str
    window_from: datetime
    window_to: datetime
    date_column: str
    dest: str
    fetch_size: int
    max_rows: int


# =============================================================================
# TIME / LOG HELPERS
# =============================================================================

def now_pht() -> datetime:
    return datetime.now(PHT_OFFSET)


def format_pht(dt: datetime) -> str:
    return dt.astimezone(PHT_OFFSET).strftime("%Y-%m-%d %H:%M:%S")


def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def duration_hhmmss(seconds: float) -> str:
    seconds_int = int(round(seconds))
    h = seconds_int // 3600
    m = (seconds_int % 3600) // 60
    s = seconds_int % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def file_date_label(window_from: datetime) -> str:
    # File/folder label must follow the exact UTC --from date used for extraction.
    return window_from.astimezone(timezone.utc).strftime("%Y%m%d")


def parse_datetime_as_utc(value: str) -> datetime:
    """
    Accepts:
      2025-11-28 06:00:00
      2025-11-28T06:00:00
      2025-11-28T06:00:00Z
      2025-11-28T06:00:00+00:00

    If timezone is missing, treat it as UTC because source MariaDB GameDate is UTC/system time.
    """
    v = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        dt = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SOURCE_TIMEZONE)

    return dt.astimezone(timezone.utc)


def mysql_datetime_utc(dt: datetime) -> str:
    """
    MariaDB/MySQL DATETIME literal in UTC/system time.

    Example input parameter:
        --from '2025-11-28 06:00:00'

    SQL bind value sent to MariaDB:
        2025-11-28 06:00:00
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{APP_NAME}_{now_pht().strftime('%Y%m%d_%H%M%S')}_PHT.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_file


# =============================================================================
# SAFETY HELPERS
# =============================================================================

def apply_low_priority() -> None:
    if not ENABLE_OS_NICE:
        return
    try:
        os.nice(OS_NICE_INCREMENT)
        logging.info("Applied low OS priority using nice +%s", OS_NICE_INCREMENT)
    except Exception as exc:
        logging.warning("Could not apply OS nice priority: %s", exc)


def acquire_lock():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    lock_handle = open(str(LOCK_FILE), "w")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_handle.write(str(os.getpid()))
        lock_handle.flush()
        return lock_handle
    except IOError:
        raise RuntimeError(f"Another extractor run is already active. Lock file: {LOCK_FILE}")


STOP_REQUESTED = False


def signal_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    logging.warning("Stop requested by signal %s. Finishing current fetch batch then stopping.", signum)


# =============================================================================
# DATA HELPERS
# =============================================================================

def quote_identifier(name: str) -> str:
    allowed = name.replace("_", "").replace("-", "")
    if not allowed.isalnum():
        raise ValueError(f"Unsafe identifier: {name}")
    return f"`{name}`"


def datetime_to_utc_z(value: datetime) -> str:
    """
    Format source MariaDB/MySQL datetime as UTC Z without shifting if it is naive.

    Important:
    - Source GameDate/UpdateDateTime values are already UTC/system time.
    - If MariaDB returns a naive datetime, attach UTC first.
    - Then write ISO milliseconds with Z.

    Example:
        MariaDB UTC: 2025-11-28 06:00:00
        JSON UTC:   2025-11-28T06:00:00.000Z
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=SOURCE_TIMEZONE)

    value_utc = value.astimezone(timezone.utc)
    return value_utc.strftime("%Y-%m-%dT%H:%M:%S") + f".{int(value_utc.microsecond / 1000):03d}Z"


def serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return datetime_to_utc_z(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.000000")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0.000000")


def decimal_6(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000001")))


def add_computed_fields(payload: dict) -> None:
    jackpot_payout = sum(to_decimal(payload.get(col)) for col in ["JW1", "JW2", "JW3", "JW4", "JW5"])
    jackpot_contribution = sum(to_decimal(payload.get(col)) for col in ["PC1", "PC2", "PC3", "PC4", "PC5"])

    payload["JACKPOT_PAYOUT"] = decimal_6(jackpot_payout)
    payload["JACKPOT_CONTRIBUTION"] = decimal_6(jackpot_contribution)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def count_gzip_jsonl_rows(path: Path) -> int:
    """Count non-empty JSONL rows in the generated gzip file."""
    count = 0
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


# =============================================================================
# ARGUMENTS
# =============================================================================

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Safe EGMS MariaDB/MySQL UTC date-window extractor.")
    parser.add_argument("--db", default=DEFAULT_DB, help="MySQL/MariaDB database name.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="MySQL/MariaDB table name.")
    parser.add_argument("--pwd", required=True, help="MySQL/MariaDB password.")
    parser.add_argument("--from", dest="window_from", required=True, help="UTC start datetime. Example: 2025-11-28 06:00:00
")
    parser.add_argument("--to", dest="window_to", required=True, help="UTC end datetime. Example: 2025-11-29 06:00:00")
    parser.add_argument("--date-column", default=DEFAULT_DATE_COLUMN, help="Date column filter. Usually GameDate or UpdateD
ateTime.")
    parser.add_argument("--dest", choices=["local", "sftp"], default=DEFAULT_DEST, help="Output destination: local or sftp.
")
    parser.add_argument("--fetch-size", type=int, default=FETCH_SIZE, help="Rows read from cursor at a time. Not total row
limit.")
    parser.add_argument("--max-rows", type=int, default=MAX_ROWS_TO_EXTRACT, help="Optional test cap. 0 means unlimited/ful
l extract.")

    args = parser.parse_args()

    window_from = parse_datetime_as_utc(args.window_from)
    window_to = parse_datetime_as_utc(args.window_to)

    if window_to <= window_from:
        raise ValueError("--to must be greater than --from")
    if args.fetch_size <= 0:
        raise ValueError("--fetch-size must be greater than zero")
    if args.date_column not in SOURCE_SELECT_COLUMNS:
        raise ValueError(f"--date-column must be one of SOURCE_SELECT_COLUMNS. Got: {args.date_column}")
    if OUTPUT_ID_COLUMN not in SOURCE_SELECT_COLUMNS:
        raise ValueError(f"OUTPUT_ID_COLUMN {OUTPUT_ID_COLUMN} must exist in SOURCE_SELECT_COLUMNS")
    if GAME_DT_OUTPUT_COLUMN not in SOURCE_SELECT_COLUMNS:
        raise ValueError(f"GAME_DT_OUTPUT_COLUMN {GAME_DT_OUTPUT_COLUMN} must exist in SOURCE_SELECT_COLUMNS")

    return Config(
        db=args.db,
        table=args.table,
        pwd=args.pwd,
        window_from=window_from,
        window_to=window_to,
        date_column=args.date_column,
        dest=args.dest,
        fetch_size=args.fetch_size,
        max_rows=args.max_rows,
    )


# =============================================================================
# MYSQL / MARIADB
# =============================================================================

def mysql_connection(cfg: Config):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=cfg.pwd,
        database=cfg.db,
        charset="utf8mb4",
        cursorclass=SSDictCursor,
        autocommit=True,
        connect_timeout=10,
        read_timeout=7200,
        write_timeout=7200,
    )


def build_select_sql(cfg: Config) -> str:
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)
    date_col = quote_identifier(cfg.date_column)
    selected_columns = ",\n            ".join(quote_identifier(col) for col in SOURCE_SELECT_COLUMNS)

    # Date-window filter only. No LIMIT. No keyset condition.
    return f"""
        SELECT
            {selected_columns}
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
    """


def get_source_count(conn, cfg: Config) -> Optional[int]:
    if not ENABLE_SOURCE_COUNT:
        return None

    return get_database_window_count(conn, cfg)


def get_database_window_count(conn, cfg: Config) -> int:
    """Count source rows for the exact UTC date window used by the extract."""
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)
    date_col = quote_identifier(cfg.date_column)
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, (mysql_datetime_utc(cfg.window_from), mysql_datetime_utc(cfg.window_to)))
        row = cur.fetchone()
        return int(row["cnt"])


# =============================================================================
# FILES
# =============================================================================

def make_run_dir(cfg: Config) -> Path:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    run_name = f"{FILE_PREFIX}_{file_date_label(cfg.window_from)}"
    run_dir = WORK_DIR / run_name

    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def output_file_name(cfg: Config) -> str:
    return f"{FILE_PREFIX}_{file_date_label(cfg.window_from)}.jsonl.gz"


def manifest_file_name(cfg: Config) -> str:
    return f"{FILE_PREFIX}_{file_date_label(cfg.window_from)}_manifest.json"


def copy_to_output(run_dir: Path) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    final_dir = OUTPUT_DIR / run_dir.name
    tmp_final = OUTPUT_DIR / f".{run_dir.name}.tmp"

    if tmp_final.exists():
        shutil.rmtree(tmp_final)
    if final_dir.exists():
        shutil.rmtree(final_dir)

    shutil.copytree(run_dir, tmp_final)
    tmp_final.rename(final_dir)
    return final_dir


def write_manifest(
    cfg: Config,
    run_dir: Path,
    output_file: Path,
    source_count: Optional[int],
    extracted_count: int,
    output_json_line_count: Optional[int],
    validation_status: str,
    started_at: datetime,
    completed_at: datetime,
) -> Path:
    duration_seconds = (completed_at - started_at).total_seconds()
    manifest = {
        "app_name": APP_NAME,
        "file_name": output_file.name,
        "mysql_host": MYSQL_HOST,
        "mysql_port": MYSQL_PORT,
        "mysql_user": MYSQL_USER,
        "mysql_database": cfg.db,
        "mysql_table": cfg.table,
        "date_column_used_for_extract": cfg.date_column,
        "target_columns": ["id", "data", "game_dt"],
        "payload_columns_order": PAYLOAD_COLUMNS,
        "datetime_output_format": "Source MariaDB/MySQL UTC datetime written as UTC ISO milliseconds with Z",
        "datetime_example": "2025-11-28 06:00:00 UTC -> 2025-11-28T06:00:00.000Z",
        "window_from_utc": format_utc(cfg.window_from),
        "window_to_utc": format_utc(cfg.window_to),
        "source_count": source_count if source_count is not None else "skipped",
        "extracted_count": extracted_count,
        "output_json_line_count": output_json_line_count if output_json_line_count is not None else "not_checked",
        "count_validation_status": validation_status,
        "fetch_size": cfg.fetch_size,
        "max_rows": cfg.max_rows,
        "started_at_pht": format_pht(started_at),
        "completed_at_pht": format_pht(completed_at),
        "duration_seconds": round(duration_seconds, 3),
        "duration_minutes": round(duration_seconds / 60.0, 3),
        "duration_hhmmss": duration_hhmmss(duration_seconds),
        "destination": cfg.dest,
        "computed_fields": {
            "JACKPOT_PAYOUT": "JW1 + JW2 + JW3 + JW4 + JW5",
            "JACKPOT_CONTRIBUTION": "PC1 + PC2 + PC3 + PC4 + PC5",
        },
        "file": {
            "file_name": output_file.name,
            "size_bytes": output_file.stat().st_size,
            "sha256": sha256_file(output_file),
        },
    }

    manifest_path = run_dir / manifest_file_name(cfg)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path


# =============================================================================
# SFTP
# =============================================================================

def ensure_sftp_dir(sftp, remote_dir: str) -> None:
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    current = ""
    for part in parts:
        current += f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def upload_sftp(local_dir: Path) -> None:
    if paramiko is None:
        raise RuntimeError("paramiko is not installed. Run: python3.9 -m pip install paramiko")
    if not SFTP_PRIVATE_KEY.exists():
        raise FileNotFoundError(f"SFTP private key not found: {SFTP_PRIVATE_KEY}")

    key = paramiko.RSAKey.from_private_key_file(str(SFTP_PRIVATE_KEY))
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, pkey=key)
    try:
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_run_dir = f"{SFTP_REMOTE_DIR.rstrip('/')}/{local_dir.name}"
        ensure_sftp_dir(sftp, remote_run_dir)
        for file_path in sorted(local_dir.iterdir()):
            if file_path.is_file():
                remote_path = f"{remote_run_dir}/{file_path.name}"
                logging.info("Uploading to SFTP: %s", remote_path)
                sftp.put(str(file_path), remote_path)
        sftp.close()
    finally:
        transport.close()


# =============================================================================
# CLEANUP
# =============================================================================

def cleanup_old_files(directory: Path, days: int, pattern: str = "*") -> None:
    if days <= 0 or not directory.exists():
        return
    cutoff = time.time() - (days * 86400)
    for path in directory.glob(pattern):
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()
            elif path.is_dir() and path.stat().st_mtime < cutoff:
                shutil.rmtree(path)
        except Exception as exc:
            logging.warning("Cleanup failed for %s: %s", path, exc)


# =============================================================================
# EXTRACT
# =============================================================================

def run_extract(cfg: Config) -> None:
    started_at = now_pht()
    run_dir = make_run_dir(cfg)
    output_path = run_dir / output_file_name(cfg)

    logging.info("Started at PHT : %s", format_pht(started_at))
    logging.info("Remote MySQL   : %s:%s", MYSQL_HOST, MYSQL_PORT)
    logging.info("Run directory  : %s", run_dir)
    logging.info("Output file    : %s", output_path.name)
    logging.info("Database       : %s", cfg.db)
    logging.info("Table          : %s", cfg.table)
    logging.info("Date column    : %s", cfg.date_column)
    logging.info("Window from UTC: %s", format_utc(cfg.window_from))
    logging.info("Window to UTC  : %s", format_utc(cfg.window_to))
    logging.info("Fetch size     : %s", cfg.fetch_size)
    logging.info("Max rows       : %s", "unlimited" if cfg.max_rows == 0 else cfg.max_rows)
    logging.info("Destination    : %s", cfg.dest)

    apply_low_priority()

    conn = mysql_connection(cfg)
    extracted_count = 0
    source_count = None
    last_progress_log = 0

    try:
        source_count = get_source_count(conn, cfg)
        if source_count is None:
            logging.info("Source count   : skipped because ENABLE_SOURCE_COUNT=False")
        else:
            logging.info("Source count   : %s", source_count)

        sql = build_select_sql(cfg)
        params = (mysql_datetime_utc(cfg.window_from), mysql_datetime_utc(cfg.window_to))

        logging.info("Executing date-window SELECT with streaming cursor. No SQL LIMIT.")
        logging.info("SQL window bind from UTC: %s", params[0])
        logging.info("SQL window bind to UTC  : %s", params[1])

        with conn.cursor() as cur:
            cur.execute(sql, params)

            with gzip.open(output_path, "wt", encoding="utf-8") as gz:
                while True:
                    if STOP_REQUESTED:
                        logging.warning("Stop requested. Ending extraction after current completed fetch.")
                        break

                    rows = cur.fetchmany(cfg.fetch_size)
                    if not rows:
                        break

                    for row in rows:
                        source_payload = {col: serialize_value(row.get(col)) for col in SOURCE_SELECT_COLUMNS}
                        add_computed_fields(source_payload)

                        # Keep the JSONB payload keys in the exact approved order.
                        # Python 3.9 preserves dict insertion order.
                        payload = {col: source_payload.get(col) for col in PAYLOAD_COLUMNS}

                        output_row = {
                            "id": serialize_value(row.get(OUTPUT_ID_COLUMN)),
                            "data": payload,
                            "game_dt": serialize_value(row.get(GAME_DT_OUTPUT_COLUMN)),
                        }

                        gz.write(json.dumps(output_row, ensure_ascii=False, separators=(",", ":")))
                        gz.write("\n")
                        extracted_count += 1

                        if cfg.max_rows > 0 and extracted_count >= cfg.max_rows:
                            logging.warning("Reached --max-rows test cap: %s", cfg.max_rows)
                            break

                    if extracted_count - last_progress_log >= PROGRESS_LOG_EVERY_ROWS:
                        logging.info("Progress       : extracted_count=%s", extracted_count)
                        last_progress_log = extracted_count

                    if cfg.max_rows > 0 and extracted_count >= cfg.max_rows:
                        break

                    if SLEEP_SECONDS_BETWEEN_FETCHES > 0:
                        time.sleep(SLEEP_SECONDS_BETWEEN_FETCHES)

        logging.info("Running final count validation...")
        logging.info("Counting generated JSONL.GZ rows...")
        output_json_line_count = count_gzip_jsonl_rows(output_path)
        logging.info("Output JSONL row count: %s", output_json_line_count)

        logging.info("Counting source database rows for the same UTC window...")
        database_window_count = get_database_window_count(conn, cfg)
        logging.info("Database window count: %s", database_window_count)

        if not (database_window_count == extracted_count == output_json_line_count):
            completed_at = now_pht()
            manifest_path = write_manifest(
                cfg=cfg,
                run_dir=run_dir,
                output_file=output_path,
                source_count=database_window_count,
                extracted_count=extracted_count,
                output_json_line_count=output_json_line_count,
                validation_status="FAIL",
                started_at=started_at,
                completed_at=completed_at,
            )
            logging.info("Manifest written: %s", manifest_path)
            logging.error(
                "FAIL: Count mismatch. database_count=%s extracted_count=%s output_json_line_count=%s",
                database_window_count,
                extracted_count,
                output_json_line_count,
            )
            print("\nEXTRACT SUMMARY")
            print("===============")
            print("status                  : FAIL")
            print(f"database_count          : {database_window_count}")
            print(f"extracted_count         : {extracted_count}")
            print(f"output_json_line_count  : {output_json_line_count}")
            print("reason                  : count mismatch between source database and output JSON file")
            raise RuntimeError(
                f"FAIL: Count mismatch. database_count={database_window_count}, "
                f"extracted_count={extracted_count}, output_json_line_count={output_json_line_count}"
            )

        logging.info(
            "PASS: database_count, extracted_count, and output_json_line_count all match: %s",
            database_window_count,
        )

        completed_at = now_pht()
        manifest_path = write_manifest(
            cfg=cfg,
            run_dir=run_dir,
            output_file=output_path,
            source_count=database_window_count,
            extracted_count=extracted_count,
            output_json_line_count=output_json_line_count,
            validation_status="PASS",
            started_at=started_at,
            completed_at=completed_at,
        )
        logging.info("Manifest written: %s", manifest_path)

        final_dir = copy_to_output(run_dir)
        logging.info("Final output directory: %s", final_dir)

        if cfg.dest == "sftp":
            upload_sftp(final_dir)
            logging.info("SFTP upload completed.")

        duration_seconds = (completed_at - started_at).total_seconds()
        logging.info("Completed at PHT: %s", format_pht(completed_at))
        logging.info("Duration        : %.3f minutes (%s)", duration_seconds / 60.0, duration_hhmmss(duration_seconds))
        logging.info("Extraction completed successfully. total_extracted=%s", extracted_count)

        print("\nEXTRACT SUMMARY")
        print("===============")
        print("status                  : PASS")
        print(f"database_count          : {database_window_count}")
        print(f"extracted_count         : {extracted_count}")
        print(f"output_json_line_count  : {output_json_line_count}")
        print(f"output_directory        : {final_dir}")
        print(f"output_file             : {final_dir / output_path.name}")
        print(f"duration_minutes        : {duration_seconds / 60.0:.3f}")
        print(f"duration_hhmmss         : {duration_hhmmss(duration_seconds)}")

    finally:
        try:
            conn.close()
        except Exception:
            pass
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    lock_handle = None
    try:
        cfg = parse_args()
        lock_handle = acquire_lock()
        run_extract(cfg)
        cleanup_old_files(OUTPUT_DIR, LOCAL_RETENTION_DAYS)
        cleanup_old_files(LOG_DIR, LOG_RETENTION_DAYS, "*.log")
        return 0
    except Exception as exc:
        logging.exception("Extraction failed: %s", exc)
        return 1
    finally:
        if lock_handle is not None:
            try:
                fcntl.flock(lock_handle, fcntl.LOCK_UN)
                lock_handle.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
