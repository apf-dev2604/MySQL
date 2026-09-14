#!/usr/bin/env python3
"""
MySQL 8.0 extractor 

Purpose:
- Extract large MySQL table safely using keyset pagination.
- Avoid OFFSET pagination.
- Avoid loading all rows into memory.
- Write compressed JSONL output.
- Create manifest.json.
- Support local directory or SFTP handoff.

PostgreSQL target shape:
    id      TEXT
    data    JSONB
    game_dt TIMESTAMPTZ

Output JSONL row shape:
    {"id": "TransactionID", "data": {...source columns...}, "game_dt": "GameDate"}

Output directory and file names use PHT date:
    /opt/mysql_extract/out/egms_games_txn_yyyymmdd/
    egms_games_txn_yyyymmdd.jsonl.gz
    egms_games_txn_yyyymmdd_manifest.json

Recommended MySQL indexes:

For daily business-date extraction:
    CREATE INDEX idx_egms_games_txn_gamedate_transactionid
    ON your_table_name (GameDate, TransactionID);

For change-aware extraction:
    CREATE INDEX idx_egms_games_txn_updatedatetime_transactionid
    ON your_table_name (UpdateDateTime, TransactionID);

Example local run using PHT window:
    python3 extract_egms_gamestx.py \
      --db source_database \
      --table source_table \
      --pwd 'CHANGE_ME' \
      --from '2025-10-23 00:00:00' \
      --to '2025-10-24 00:00:00' \
      --batch-size 10000 \
      --date-column GameDate \
      --dest local
"""

import argparse
import gzip
import hashlib
import json
import logging
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pymysql

try:
    import paramiko
except ImportError:
    paramiko = None


# =============================================================================
# CHANGE VARIABLES HERE
# =============================================================================

MYSQL_HOST = "10.0.0.10"
MYSQL_PORT = 3306
MYSQL_USER = "readonly_user"

DEFAULT_DB = "source_database"
DEFAULT_TABLE = "source_table"

# Local/business timezone used for filename and for no-timezone command input.
LOCAL_TZ_NAME = "Asia/Manila"
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

# If MySQL DATETIME values are stored in UTC, keep this True.
# If MySQL DATETIME values are already stored as PHT local time, set this False.
MYSQL_STORES_UTC = True

# PostgreSQL id = MySQL TransactionID.
ID_COLUMN = "TransactionID"

# Default extraction date column.
# Use GameDate for daily business-date extraction.
# Use UpdateDateTime for change-aware extraction.
DEFAULT_DATE_COLUMN = "GameDate"

# Final output base name.
FILE_PREFIX = "egms_games_txn"

# All source fields included inside PostgreSQL JSONB data column.
DATA_COLUMNS = [
    "Idx",
    "GameProvider",
    "TransactionID",
    "SessionID",
    "GameDate",
    "Outlet",
    "PlayerAccount",
    "GameName",
    "TotalStakes",
    "TotalWins",
    "PC1",
    "PC2",
    "PC3",
    "PC4",
    "PC5",
    "JW1",
    "JW2",
    "JW3",
    "JW4",
    "JW5",
    "UpdateDateTime",
    "JACKPOT_PAYOUT",
    "JACKPOT_CONTRIBUTION",
    "PROGRESSIVE_CONTRIBUTION_PAID",
    "SEED_MONEY_WON",
    "SEED_MONEY_JACKPOT_WON_OVER_1000",
]

BASE_DIR = Path("/opt/mysql_extract")
OUTPUT_DIR = BASE_DIR / "out"
WORK_DIR = BASE_DIR / "work"
LOG_DIR = BASE_DIR / "logs"

DEFAULT_DEST = "local"

SFTP_HOST = "sftp.example.com"
SFTP_PORT = 22
SFTP_USER = "etluser"
SFTP_PRIVATE_KEY = Path("/opt/mysql_extract/keys/sftp_id_rsa")
SFTP_REMOTE_DIR = "/incoming/mysql_extract"

DEFAULT_BATCH_SIZE = 10000
SLEEP_SECONDS_BETWEEN_BATCHES = 0.2

LOCAL_RETENTION_DAYS = 14
LOG_RETENTION_DAYS = 30

APP_NAME = "extract_egms_gamestx"


@dataclass
class Config:
    db: str
    table: str
    pwd: str
    window_from_pht: datetime
    window_to_pht: datetime
    window_from_mysql: datetime
    window_to_mysql: datetime
    batch_size: int
    date_column: str
    dest: str


def now_pht() -> datetime:
    return datetime.now(LOCAL_TZ)


def parse_input_datetime(value: str) -> datetime:
    """
    Accepts:
    - 2025-10-23 00:00:00       -> treated as PHT
    - 2025-10-23T00:00:00       -> treated as PHT
    - 2025-10-23T00:00:00+08:00 -> uses provided offset, converted to PHT
    - 2025-10-22T16:00:00Z      -> uses UTC offset, converted to PHT
    """
    raw = value.strip()
    cleaned = raw.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)

    return dt.astimezone(LOCAL_TZ)


def to_mysql_datetime(value_pht: datetime) -> datetime:
    """
    Returns the datetime value to use in MySQL WHERE predicates.

    If MySQL stores UTC DATETIME values, PHT input is converted to UTC.
    If MySQL stores local PHT DATETIME values, PHT input is kept as PHT wall time.
    """
    if MYSQL_STORES_UTC:
        return value_pht.astimezone(timezone.utc).replace(tzinfo=None)

    return value_pht.astimezone(LOCAL_TZ).replace(tzinfo=None)


def mysql_datetime_string(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def file_date_label_pht(window_from_pht: datetime) -> str:
    return window_from_pht.astimezone(LOCAL_TZ).strftime("%Y%m%d")


def quote_identifier(name: str) -> str:
    allowed = name.replace("_", "").replace("-", "")
    if not allowed.isalnum():
        raise ValueError(f"Unsafe identifier: {name}")
    return f"`{name}`"


def serialize_value(value: Any) -> Any:
    """
    JSON-safe serializer.

    Decimal values are stored as strings to avoid precision loss.
    Date/time values are stored in ISO format.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            if MYSQL_STORES_UTC:
                value = value.replace(tzinfo=timezone.utc)
            else:
                value = value.replace(tzinfo=LOCAL_TZ)
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return str(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / f"{APP_NAME}_{now_pht().strftime('%Y%m%dT%H%M%S_PHT')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s PHT | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return log_file


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Extract EGMS game transactions from MySQL into PostgreSQL-ready JSONL.GZ."
    )

    parser.add_argument("--db", default=DEFAULT_DB, help="MySQL database name.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="MySQL table name.")
    parser.add_argument("--pwd", required=True, help="MySQL password.")

    parser.add_argument(
        "--from",
        dest="window_from",
        required=True,
        help="PHT start datetime if no timezone is supplied. Example: '2025-10-23 00:00:00'",
    )

    parser.add_argument(
        "--to",
        dest="window_to",
        required=True,
        help="PHT end datetime if no timezone is supplied. Example: '2025-10-24 00:00:00'",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Rows per batch. Default: 10000",
    )

    parser.add_argument(
        "--date-column",
        default=DEFAULT_DATE_COLUMN,
        help="Date column used for extraction window. Usually GameDate or UpdateDateTime.",
    )

    parser.add_argument(
        "--dest",
        choices=["local", "sftp"],
        default=DEFAULT_DEST,
        help="Output destination: local or sftp.",
    )

    args = parser.parse_args()

    window_from_pht = parse_input_datetime(args.window_from)
    window_to_pht = parse_input_datetime(args.window_to)

    if window_to_pht <= window_from_pht:
        raise ValueError("--to must be greater than --from")

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than zero")

    if args.date_column not in DATA_COLUMNS:
        raise ValueError(
            f"--date-column must be included in DATA_COLUMNS. Got: {args.date_column}"
        )

    if ID_COLUMN not in DATA_COLUMNS:
        raise ValueError(f"ID_COLUMN {ID_COLUMN} must be included in DATA_COLUMNS")

    return Config(
        db=args.db,
        table=args.table,
        pwd=args.pwd,
        window_from_pht=window_from_pht,
        window_to_pht=window_to_pht,
        window_from_mysql=to_mysql_datetime(window_from_pht),
        window_to_mysql=to_mysql_datetime(window_to_pht),
        batch_size=args.batch_size,
        date_column=args.date_column,
        dest=args.dest,
    )


def mysql_connection(cfg: Config):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=cfg.pwd,
        database=cfg.db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        read_timeout=3600,
        write_timeout=3600,
        connect_timeout=10,
    )


def get_source_count(conn, cfg: Config) -> int:
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)
    date_col = quote_identifier(cfg.date_column)

    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                mysql_datetime_string(cfg.window_from_mysql),
                mysql_datetime_string(cfg.window_to_mysql),
            ),
        )
        row = cur.fetchone()

    return int(row["cnt"])


def fetch_batch(
    conn,
    cfg: Config,
    last_date_value: Optional[datetime],
    last_id: Optional[str],
) -> list[dict[str, Any]]:
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)

    id_col = quote_identifier(ID_COLUMN)
    date_col = quote_identifier(cfg.date_column)

    selected_columns = ",\n            ".join(
        f"{quote_identifier(col)} AS {quote_identifier(col)}"
        for col in DATA_COLUMNS
    )

    params: list[Any] = [
        mysql_datetime_string(cfg.window_from_mysql),
        mysql_datetime_string(cfg.window_to_mysql),
    ]

    if last_date_value is None or last_id is None:
        cursor_filter = ""
        params.append(cfg.batch_size)
    else:
        cursor_filter = f"""
          AND (
                {date_col} > %s
                OR ({date_col} = %s AND {id_col} > %s)
              )
        """
        params.extend(
            [
                mysql_datetime_string(last_date_value),
                mysql_datetime_string(last_date_value),
                last_id,
                cfg.batch_size,
            ]
        )

    sql = f"""
        SELECT
            {selected_columns}
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
          {cursor_filter}
        ORDER BY {date_col} ASC, {id_col} ASC
        LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def run_dir_name(cfg: Config) -> str:
    return f"{FILE_PREFIX}_{file_date_label_pht(cfg.window_from_pht)}"


def output_file_name(cfg: Config) -> str:
    return f"{FILE_PREFIX}_{file_date_label_pht(cfg.window_from_pht)}.jsonl.gz"


def manifest_file_name(cfg: Config) -> str:
    return f"{FILE_PREFIX}_{file_date_label_pht(cfg.window_from_pht)}_manifest.json"


def make_run_dir(cfg: Config) -> Path:
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    run_dir = WORK_DIR / run_dir_name(cfg)

    if run_dir.exists():
        shutil.rmtree(run_dir)

    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def write_jsonl_file(rows: list[dict[str, Any]], gz, cfg: Config) -> None:
    for row in rows:
        source_id = row.get(ID_COLUMN)
        source_game_dt = row.get("GameDate")

        data_payload = {
            col: serialize_value(row.get(col))
            for col in DATA_COLUMNS
        }

        output_row = {
            "id": serialize_value(source_id),
            "data": data_payload,
            "game_dt": serialize_value(source_game_dt),
        }

        gz.write(json.dumps(output_row, ensure_ascii=False, separators=(",", ":")))
        gz.write("\n")


def write_manifest(
    cfg: Config,
    run_dir: Path,
    output_file: Path,
    source_count: int,
    extracted_count: int,
    started_at_pht: datetime,
    completed_at_pht: datetime,
    last_date_value: Optional[datetime],
    last_id: Optional[str],
) -> Path:
    manifest = {
        "app_name": APP_NAME,
        "file_name": output_file.name,
        "timezone_used_for_file_date": LOCAL_TZ_NAME,
        "mysql_datetime_storage_assumed_utc": MYSQL_STORES_UTC,
        "mysql_host": MYSQL_HOST,
        "mysql_port": MYSQL_PORT,
        "mysql_user": MYSQL_USER,
        "mysql_database": cfg.db,
        "mysql_table": cfg.table,
        "id_column": ID_COLUMN,
        "date_column_used_for_extract": cfg.date_column,
        "target_columns": ["id", "data", "game_dt"],
        "window_from_pht": cfg.window_from_pht.isoformat(),
        "window_to_pht": cfg.window_to_pht.isoformat(),
        "window_from_mysql": mysql_datetime_string(cfg.window_from_mysql),
        "window_to_mysql": mysql_datetime_string(cfg.window_to_mysql),
        "source_count": source_count,
        "extracted_count": extracted_count,
        "last_cursor_date_value": serialize_value(last_date_value),
        "last_cursor_id": last_id,
        "started_at_pht": started_at_pht.isoformat(),
        "completed_at_pht": completed_at_pht.isoformat(),
        "destination": cfg.dest,
        "file": {
            "file_name": output_file.name,
            "size_bytes": output_file.stat().st_size,
            "sha256": sha256_file(output_file),
        },
    }

    manifest_path = run_dir / manifest_file_name(cfg)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return manifest_path


def copy_to_output(run_dir: Path) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    final_dir = OUTPUT_DIR / run_dir.name
    temp_dir = OUTPUT_DIR / f".{run_dir.name}.tmp"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    if final_dir.exists():
        raise FileExistsError(
            f"Final output directory already exists: {final_dir}. "
            "Remove it first or move it before rerunning the same date."
        )

    shutil.copytree(run_dir, temp_dir)
    temp_dir.rename(final_dir)

    return final_dir


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
        raise RuntimeError("paramiko is not installed. Run: pip install paramiko")

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


def cleanup_old_files(directory: Path, days: int, pattern: str = "*") -> None:
    if days <= 0 or not directory.exists():
        return

    cutoff = time.time() - (days * 86400)

    for path in directory.glob(pattern):
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                logging.info("Deleting old file: %s", path)
                path.unlink()
            elif path.is_dir() and path.stat().st_mtime < cutoff:
                logging.info("Deleting old directory: %s", path)
                shutil.rmtree(path)
        except Exception as exc:
            logging.warning("Cleanup failed for %s: %s", path, exc)


def run_extract(cfg: Config) -> None:
    started_at_pht = now_pht()
    run_dir = make_run_dir(cfg)
    output_path = run_dir / output_file_name(cfg)

    logging.info("Run directory                : %s", run_dir)
    logging.info("Output file                  : %s", output_path.name)
    logging.info("Database                     : %s", cfg.db)
    logging.info("Table                        : %s", cfg.table)
    logging.info("Date column                  : %s", cfg.date_column)
    logging.info("ID column                    : %s", ID_COLUMN)
    logging.info("Window from PHT              : %s", cfg.window_from_pht.isoformat())
    logging.info("Window to PHT                : %s", cfg.window_to_pht.isoformat())
    logging.info("Window from used in MySQL    : %s", mysql_datetime_string(cfg.window_from_mysql))
    logging.info("Window to used in MySQL      : %s", mysql_datetime_string(cfg.window_to_mysql))
    logging.info("MySQL stores UTC assumption  : %s", MYSQL_STORES_UTC)
    logging.info("Batch size                   : %s", cfg.batch_size)
    logging.info("Destination                  : %s", cfg.dest)

    total_extracted = 0
    batch_no = 1

    last_date_value: Optional[datetime] = None
    last_id: Optional[str] = None

    conn = mysql_connection(cfg)

    try:
        source_count = get_source_count(conn, cfg)
        logging.info("Source count: %s", source_count)

        with gzip.open(output_path, "wt", encoding="utf-8") as gz:
            while True:
                rows = fetch_batch(
                    conn=conn,
                    cfg=cfg,
                    last_date_value=last_date_value,
                    last_id=last_id,
                )

                if not rows:
                    break

                write_jsonl_file(rows, gz, cfg)

                total_extracted += len(rows)

                last_row = rows[-1]
                last_date_value = last_row[cfg.date_column]
                last_id = str(last_row[ID_COLUMN])

                logging.info(
                    "Batch extracted | batch=%s | rows=%s | total=%s | last_%s=%s | last_%s=%s",
                    batch_no,
                    len(rows),
                    total_extracted,
                    cfg.date_column,
                    last_date_value,
                    ID_COLUMN,
                    last_id,
                )

                batch_no += 1

                if SLEEP_SECONDS_BETWEEN_BATCHES > 0:
                    time.sleep(SLEEP_SECONDS_BETWEEN_BATCHES)

        completed_at_pht = now_pht()

        manifest_path = write_manifest(
            cfg=cfg,
            run_dir=run_dir,
            output_file=output_path,
            source_count=source_count,
            extracted_count=total_extracted,
            started_at_pht=started_at_pht,
            completed_at_pht=completed_at_pht,
            last_date_value=last_date_value,
            last_id=last_id,
        )

        logging.info("Manifest written: %s", manifest_path)

        final_dir = copy_to_output(run_dir)
        logging.info("Final output directory: %s", final_dir)

        if cfg.dest == "sftp":
            upload_sftp(final_dir)
            logging.info("SFTP upload completed.")

        if source_count != total_extracted:
            logging.warning(
                "Source count and extracted count differ. source_count=%s extracted_count=%s",
                source_count,
                total_extracted,
            )

        logging.info("Extraction completed successfully. total_extracted=%s", total_extracted)

    finally:
        conn.close()

        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)


def main() -> int:
    setup_logging()

    try:
        cfg = parse_args()
        run_extract(cfg)

        cleanup_old_files(OUTPUT_DIR, LOCAL_RETENTION_DAYS)
        cleanup_old_files(LOG_DIR, LOG_RETENTION_DAYS, "*.log")

        return 0

    except Exception as exc:
        logging.exception("Extraction failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
