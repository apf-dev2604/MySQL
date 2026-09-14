#!/usr/bin/env python3
"""
Purpose:
- Extract large MySQL table safely using keyset pagination.
- No OFFSET.
- No table locking.
- Writes compressed JSONL part files.
- Creates manifest.json.
- Supports local handoff or SFTP handoff.

Expected source columns:
- id      = TransactionID
- data    = JSON payload
- game_dt = GameDate

Recommended MySQL index:
    CREATE INDEX idx_table_game_dt_id
    ON your_table_name (game_dt, id);

Example command:
    python3 mysql_daily_jsonb_extract.py \
      --db source_database \
      --table source_table \
      --pwd 'CHANGE_ME' \
      --from '2025-10-23T00:00:00Z' \
      --to '2025-10-24T00:00:00Z' \
      --batch-size 10000 \
      --date-column game_dt \
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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

# These can be overridden by command-line flags.
DEFAULT_DB = "source_database"
DEFAULT_TABLE = "source_table"

# Source column mapping.
# id      = TransactionID
# data    = full JSON payload
# game_dt = GameDate
ID_COLUMN = "id"
JSON_COLUMN = "data"
DEFAULT_DATE_COLUMN = "game_dt"

# Output folders.
BASE_DIR = Path("/opt/mysql_extract")
OUTPUT_DIR = BASE_DIR / "out"
WORK_DIR = BASE_DIR / "work"
LOG_DIR = BASE_DIR / "logs"

# Local or SFTP.
DEFAULT_DEST = "local"

# SFTP settings.
SFTP_HOST = "sftp.example.com"
SFTP_PORT = 22
SFTP_USER = "etluser"
SFTP_PRIVATE_KEY = Path("/opt/mysql_extract/keys/sftp_id_rsa")
SFTP_REMOTE_DIR = "/incoming/mysql_extract"

# Extraction settings.
DEFAULT_BATCH_SIZE = 10000
SLEEP_SECONDS_BETWEEN_BATCHES = 0.2

# Retention.
LOCAL_RETENTION_DAYS = 14
LOG_RETENTION_DAYS = 30

# Naming only.
APP_NAME = "mysql_daily_jsonb_extract"
SOURCE_SYSTEM = "mysql_host"
SOURCE_ENTITY = "gameTxJsonb"


@dataclass
class Config:
    db: str
    table: str
    pwd: str
    window_from: datetime
    window_to: datetime
    batch_size: int
    date_column: str
    dest: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / f"{APP_NAME}_{utc_now().strftime('%Y%m%dT%H%M%SZ')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return log_file


def parse_utc_datetime(value: str) -> datetime:
    value = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        raise ValueError(f"Datetime must include timezone: {value}")

    return dt.astimezone(timezone.utc)


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Large-table MySQL JSON extractor using keyset pagination."
    )

    parser.add_argument("--db", default=DEFAULT_DB, help="MySQL database name.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="MySQL table name.")
    parser.add_argument("--pwd", required=True, help="MySQL password.")

    parser.add_argument("--from", dest="window_from", required=True, help="UTC start. Example: 2025-10-23T00:00:00Z")
    parser.add_argument("--to", dest="window_to", required=True, help="UTC end. Example: 2025-10-24T00:00:00Z")

    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--date-column", default=DEFAULT_DATE_COLUMN)

    parser.add_argument("--dest", choices=["local", "sftp"], default=DEFAULT_DEST)

    args = parser.parse_args()

    window_from = parse_utc_datetime(args.window_from)
    window_to = parse_utc_datetime(args.window_to)

    if window_to <= window_from:
        raise ValueError("--to must be greater than --from")

    return Config(
        db=args.db,
        table=args.table,
        pwd=args.pwd,
        window_from=window_from,
        window_to=window_to,
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


def quote_identifier(name: str) -> str:
    allowed = name.replace("_", "").replace("-", "")
    if not allowed.isalnum():
        raise ValueError(f"Unsafe identifier: {name}")
    return f"`{name}`"


def safe_json_load(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return value

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


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
                cfg.window_from.strftime("%Y-%m-%d %H:%M:%S"),
                cfg.window_to.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        row = cur.fetchone()

    return int(row["cnt"])


def fetch_batch(
    conn,
    cfg: Config,
    last_game_dt: Optional[datetime],
    last_id: Optional[str],
) -> list[dict[str, Any]]:
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)

    id_col = quote_identifier(ID_COLUMN)
    json_col = quote_identifier(JSON_COLUMN)
    date_col = quote_identifier(cfg.date_column)

    params = [
        cfg.window_from.strftime("%Y-%m-%d %H:%M:%S"),
        cfg.window_to.strftime("%Y-%m-%d %H:%M:%S"),
    ]

    if last_game_dt is None or last_id is None:
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
                last_game_dt.strftime("%Y-%m-%d %H:%M:%S"),
                last_game_dt.strftime("%Y-%m-%d %H:%M:%S"),
                last_id,
                cfg.batch_size,
            ]
        )

    sql = f"""
        SELECT
            {id_col} AS source_id,
            {json_col} AS source_payload,
            {date_col} AS source_game_dt
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


def make_run_dir(cfg: Config) -> Path:
    label_from = cfg.window_from.strftime("%Y%m%dT%H%M%SZ")
    label_to = cfg.window_to.strftime("%Y%m%dT%H%M%SZ")
    run_ts = utc_now().strftime("%Y%m%dT%H%M%SZ")

    run_name = (
        f"{SOURCE_SYSTEM}.{SOURCE_ENTITY}."
        f"{cfg.date_column}_{label_from}_{label_to}.{run_ts}"
    )

    run_dir = WORK_DIR / run_name
    run_dir.mkdir(parents=True, exist_ok=False)

    return run_dir


def write_part_file(
    rows: list[dict[str, Any]],
    run_dir: Path,
    part_no: int,
) -> Path:
    part_file = run_dir / f"part-{part_no:05d}.jsonl.gz"

    with gzip.open(part_file, "wt", encoding="utf-8") as gz:
        for row in rows:
            game_dt = row["source_game_dt"]

            output_row = {
                "id": str(row["source_id"]),
                "data": safe_json_load(row["source_payload"]),
                "game_dt": game_dt.isoformat() if hasattr(game_dt, "isoformat") else str(game_dt),
            }

            gz.write(json.dumps(output_row, ensure_ascii=False, separators=(",", ":")))
            gz.write("\n")

    return part_file


def write_manifest(
    cfg: Config,
    run_dir: Path,
    source_count: int,
    extracted_count: int,
    part_files: list[Path],
    started_at: datetime,
    completed_at: datetime,
) -> Path:
    manifest = {
        "app_name": APP_NAME,
        "source_system": SOURCE_SYSTEM,
        "source_entity": SOURCE_ENTITY,
        "mysql_host": MYSQL_HOST,
        "mysql_port": MYSQL_PORT,
        "mysql_user": MYSQL_USER,
        "mysql_database": cfg.db,
        "mysql_table": cfg.table,
        "id_column": ID_COLUMN,
        "json_column": JSON_COLUMN,
        "date_column": cfg.date_column,
        "window_from_utc": cfg.window_from.isoformat(),
        "window_to_utc": cfg.window_to.isoformat(),
        "source_count": source_count,
        "extracted_count": extracted_count,
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "destination": cfg.dest,
        "files": [],
    }

    for f in part_files:
        manifest["files"].append(
            {
                "file_name": f.name,
                "size_bytes": f.stat().st_size,
                "sha256": sha256_file(f),
            }
        )

    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    return manifest_path


def copy_to_output(run_dir: Path) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    final_dir = OUTPUT_DIR / run_dir.name
    temp_dir = OUTPUT_DIR / f".{run_dir.name}.tmp"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

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
    if not directory.exists():
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


def run_extract(cfg: Config) -> None:
    started_at = utc_now()
    run_dir = make_run_dir(cfg)

    logging.info("Run directory : %s", run_dir)
    logging.info("Database      : %s", cfg.db)
    logging.info("Table         : %s", cfg.table)
    logging.info("Date column   : %s", cfg.date_column)
    logging.info("ID column     : %s", ID_COLUMN)
    logging.info("JSON column   : %s", JSON_COLUMN)
    logging.info("Window from   : %s", cfg.window_from.isoformat())
    logging.info("Window to     : %s", cfg.window_to.isoformat())
    logging.info("Batch size    : %s", cfg.batch_size)
    logging.info("Destination   : %s", cfg.dest)

    total_extracted = 0
    part_no = 1
    part_files: list[Path] = []

    last_game_dt: Optional[datetime] = None
    last_id: Optional[str] = None

    conn = mysql_connection(cfg)

    try:
        source_count = get_source_count(conn, cfg)
        logging.info("Source count: %s", source_count)

        while True:
            rows = fetch_batch(conn, cfg, last_game_dt, last_id)

            if not rows:
                break

            part_file = write_part_file(rows, run_dir, part_no)
            part_files.append(part_file)

            total_extracted += len(rows)

            last_row = rows[-1]
            last_game_dt = last_row["source_game_dt"]
            last_id = str(last_row["source_id"])

            logging.info(
                "Part %s written | rows=%s | total=%s | last_game_dt=%s | last_id=%s",
                part_file.name,
                len(rows),
                total_extracted,
                last_game_dt,
                last_id,
            )

            part_no += 1

            if SLEEP_SECONDS_BETWEEN_BATCHES > 0:
                time.sleep(SLEEP_SECONDS_BETWEEN_BATCHES)

        completed_at = utc_now()

        manifest_path = write_manifest(
            cfg=cfg,
            run_dir=run_dir,
            source_count=source_count,
            extracted_count=total_extracted,
            part_files=part_files,
            started_at=started_at,
            completed_at=completed_at,
        )

        logging.info("Manifest written: %s", manifest_path)

        final_dir = copy_to_output(run_dir)
        logging.info("Final output directory: %s", final_dir)

        if cfg.dest == "sftp":
            upload_sftp(final_dir)
            logging.info("SFTP upload completed.")

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
