EGMS Game Transactions Extraction and Loading Script

Overview

Extraction

This script extracts EGMS game transaction records from the source MariaDB/MySQL database and writes the result into a compressed JSONL file that is ready for PostgreSQL loading.
This script is the Extract stage of the ETL workflow.
It does the following:

1. Connects to the source MariaDB/MySQL server.

2. Extracts rows from Artemis.EGMS_Game_Trans using a date-window filter.

3. Uses a streaming cursor and fetchmany() so the full dataset is not loaded into memory.

4. Writes output as .jsonl.gz.

Produces PostgreSQL-ready rows with this structure:
{"id":"...","data":{...},"game_dt":"..."}
Treats source GameDate / UpdateDateTime values as UTC system time.
Writes datetime values in UTC ISO format with Z.
Performs a final count validation between source database count, extracted count, and output JSONL line count.
Prints PASS only when all counts match.

ETL Qualification
Yes, this qualifies as part of an ETL script.
More specifically, this is the Extraction script in the ETL pipeline:

ETL Stage

Covered by this script?
Description
Extract and Reads data from MariaDB/MySQL using a date-window query.

Transform: Partial
Converts source rows into JSON structure, computes jackpot fields, and formats datetime values.

Loading into PostgreSQL is handled by the separate loader script.
Together with the PostgreSQL loader script, this becomes a full ETL process.

Expected Source Query Pattern
The script extracts records using a date-window query like this:

SELECT COUNT(*) AS remaining_rows
FROM Artemis.EGMS_Game_Trans
WHERE GameDate >= '2025-11-28 06:00:00'
  AND GameDate <  '2025-11-29 06:00:00';

The script uses parameter binding internally, but the logic is equivalent to the example above.

Source Timezone Rule

The source MariaDB/MySQL datetime values are treated as UTC system time.

Example:

Source GameDate:
2025-11-28 06:00:00

Output JSON value:
2025-11-28T06:00:00.000Z

The script does not subtract 8 hours.

Output Directory Structure

Default output directory:

/home/allanf/scripts/artem/out

For a run with --from '2025-11-28 06:00:00', the output is expected to be:

/home/allanf/scripts/artem/out/egms_games_txn_20251128/egms_games_txn_20251128.jsonl.gz

A manifest file is also written in the same output folder.

Prerequisites

Operating System
Recommended:

Linux server or EC2 instance
Python 3.9+
Network access to the source MariaDB/MySQL server

Python Packages
Install dependencies inside a Python virtual environment:
python3.9 -m venv /home/allanf/scripts/.venv
source /home/allanf/scripts/.venv/bin/activate
pip install --upgrade pip
pip install pymysql paramiko

paramiko is only required if SFTP upload is used.

Deployment Steps

1. Create script directory

mkdir -p /home/allanf/scripts/artem
mkdir -p /home/allanf/scripts/artem/out
mkdir -p /home/allanf/scripts/artem/work
mkdir -p /home/allanf/scripts/artem/logs

2. Copy the extraction script

Copy the Python file to:

/home/allanf/scripts/artem/egms_games_txn_mysql_extract.py

3. Set permission

chmod 750 /home/allanf/scripts/artem/egms_games_txn_mysql_extract.py

4. Activate Python environment

source /home/allanf/scripts/.venv/bin/activate

Basic Run Command

Example for one business/date window:

python3.9 /home/allanf/scripts/artem/egms_games_txn_mysql_extract.py \
  --db Artemis \
  --table EGMS_Game_Trans \
  --pwd "$SRC_PWD" \
  --from '2025-11-28 06:00:00' \
  --to '2025-11-29 06:00:00' \
  --date-column GameDate \
  --dest local

Password Handling

Recommended runtime password input:

read -r -s -p "MariaDB password: " SRC_PWD
echo

Then run the script with:

--pwd "$SRC_PWD"

Avoid hardcoding production passwords inside bash scripts.

Validation Behavior

At the end of the run, the script validates:

source_database_count == extracted_count == output_jsonl_line_count


If all counts match, status is:
PASS
If any count does not match, status is:

FAIL
This prevents silent partial extraction.

Recommended Source Index
For GameDate extraction:

CREATE INDEX idx_egms_gamedate
ON Artemis.EGMS_Game_Trans (GameDate);

For UpdateDateTime extraction:

CREATE INDEX idx_egms_updatedatetime
ON Artemis.EGMS_Game_Trans (UpdateDateTime);

Common Errors and Fixes

Error: Access denied for user
Check the MariaDB username, password, and source IP whitelist.

Error: Connection timed out
Check firewall, VPN, security group, or database network access.

Error: Output count mismatch
Do not load the file. Re-run extraction for the same window and compare the source count manually.

Error: Another extractor run is already active
The lock file is preventing concurrent runs. Confirm no active process is running before removing the lock file.

Operational Notes

The script is safe for large extracts because it uses streaming cursor reads.

FETCH_SIZE controls memory batch size only; it is not a row limit.
MAX_ROWS_TO_EXTRACT should remain 0 for full production extraction.
The output file date label is based on the --from date.

#================================================================#

Loader

Overview

This script loads EGMS extracted JSONL.GZ files into PostgreSQL.This script is the Load stage of the ETL workflow.

It does the following:

1. Reads the extractor output file from /home/allanf/scripts/artem/out.
2. Counts JSONL rows before loading.
3. Truncates and reloads the transient table.
4. Loads records into the transient table using PostgreSQL COPY.
5.Validates that file count, copied count, and transient table count match.

Optionally inserts records into a selected final table.
Uses idempotent insert logic for final table loading.
Copies rows into a history/replay table using idempotent logic.
Writes skipped records to load_skip_out when records already exist in the final table.

ETL Qualification

Yes, this qualifies as part of an ETL script.

More specifically, this is the Load script in the ETL pipeline:

ETL Stage: Loading


Description
Extraction is handled by the MariaDB/MySQL extractor script.

Transform: Partial

Normalizes JSON text and validates row structure before loading.
Loads data into PostgreSQL temp, final, and history tables.

Together with the extraction script, this is a complete ETL pipeline.

Input File Pattern

The loader expects the extraction output file to follow this pattern:
/home/allanf/scripts/artem/out/egms_games_txn_yyyymmdd/egms_games_txn_yyyymmdd.jsonl.gz

Example:

/home/allanf/scripts/artem/out/egms_games_txn_20251128/egms_games_txn_20251128.jsonl.gz

The yyyymmdd value comes from the loader --date flag.

Expected JSONL Row Format

Each line must contain:

{"id":"...","data":{...},"game_dt":"..."}

The loader copies only these fields into PostgreSQL:

id
data
game_dt

Target Table Requirements
The transient table, final table, and history table must already exist.
The loader does not create tables automatically.

Transient Table:

CREATE TABLE public.temp_egms_games_txn (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    game_dt TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

Final Table

Minimum required columns:

id TEXT PRIMARY KEY,
data JSONB NOT NULL,
game_dt TIMESTAMPTZ NOT NULL

Example:

CREATE TABLE public."GameTransactionInplayV1" (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    game_dt TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

History Table

Minimum recommended structure:

CREATE TABLE public.temp_egms_games_txn_history (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    game_dt TIMESTAMPTZ NOT NULL,
    copied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

The history table should have a primary key or unique constraint on id so the loader can preserve history using:

ON CONFLICT (id) DO NOTHING

PostgreSQL Privileges

Run as PostgreSQL admin or RDS master user:

GRANT CONNECT ON DATABASE iestdl TO egms_loader;
GRANT USAGE ON SCHEMA public TO egms_loader;

GRANT SELECT, INSERT, TRUNCATE
ON TABLE public.temp_egms_games_txn
TO egms_loader;

GRANT SELECT, INSERT
ON TABLE public."GameTransactionInplayV1"
TO egms_loader;

GRANT SELECT, INSERT
ON TABLE public.temp_egms_games_txn_history
TO egms_loader;

If using another final table, grant privileges on that table also.

Prerequisites

Operating System

Recommended:

Linux server or EC2 instance
Python 3.9+
Network access to PostgreSQL/RDS

Python Packages

Install dependencies:

python3.9 -m venv /home/allanf/scripts/.venv
source /home/allanf/scripts/.venv/bin/activate
pip install --upgrade pip
pip install psycopg2-binary

Deployment Steps

1. Create loader directory

mkdir -p /home/allanf/scripts/artem
mkdir -p /home/allanf/scripts/artem/pg_loader/logs
mkdir -p /home/allanf/scripts/artem/pg_loader/load_skip_out

2. Copy the loader script

Copy the Python file to:

/home/allanf/scripts/artem/egms_games_txn_pg_loader.py

3. Set permission

chmod 750 /home/allanf/scripts/artem/egms_games_txn_pg_loader.py

4. Activate Python environment

source /home/allanf/scripts/.venv/bin/activate

Password Handling

Recommended runtime password input:

read -r -s -p "PostgreSQL password: " PG_PWD
echo

Then pass it to the script:

--pg-password "$PG_PWD"

Check Only Mode

Use this first to validate connection, table resolution, columns, and privileges:

python3.9 /home/allanf/scripts/artem/egms_games_txn_pg_loader.py \
  --date 20251128 \
  --pg-password "$PG_PWD" \
  --final \
  --to-schema public \
  --to-table GameTransactionInplayV1 \
  --check-only

This does not read the file, truncate the temp table, copy rows, or insert final records.

Temp Validation Only

This loads and validates the transient table only.

python3.9 /home/allanf/scripts/artem/egms_games_txn_pg_loader.py \
  --date 20251128 \
  --pg-password "$PG_PWD"

Expected successful summary:

LOAD SUMMARY
============
status                    : PASS
migration_status          : GOOD FOR MIGRATION

This means the JSON file was loaded into temp and the counts matched.

Final Load Mode

This loads temp and then inserts new rows into the selected final table:

python3.9 /home/allanf/scripts/artem/egms_games_txn_pg_loader.py \
  --date 20251128 \
  --pg-password "$PG_PWD" \
  --final \
  --to-schema public \
  --to-table GameTransactionInplayV1

Expected successful summary:

LOAD SUMMARY
============
status                    : PASS
migration_status          : LOADED IN "public"."GameTransactionInplayV1"

Idempotent Final Insert
The final table load uses:
ON CONFLICT (id) DO NOTHING

Meaning:

Scenario

Result
ID does not exist in final table
Inserted

ID already exists in final table
Skipped

Re-run same file
Safe; duplicates are skipped

Idempotent History Insert
The history copy also uses idempotent insert logic:
ON CONFLICT (id) DO NOTHING

This preserves what was already inserted into history and avoids duplicate history rows during reruns.

Skipped Record Output
If records already exist in the final table before insert, the loader writes them to:
/home/allanf/scripts/artem/pg_loader/load_skip_out/egms_games_txn_yyyymmdd_skipped_existing_in_final.jsonl

Each skipped row contains only:
{"id":"...","game_dt":"...","loaded_time":"..."}

The loaded_time value is generated during the loader run.

Can This Loader Be Used for Another Table?

Yes.
The loader is not locked to only one final table. The destination is controlled by:

--to-schema public
--to-table GameTransactionInplayV1

You can use another table as long as it has the required columns:

id
data
game_dt

The id column must have a primary key or unique constraint because the loader depends on ON CONFLICT (id) DO NOTHING.

Example:

python3.9 /home/allanf/scripts/artem/egms_games_txn_pg_loader.py \
  --date 20251128 \
  --pg-password "$PG_PWD" \
  --final \
  --to-schema public \
  --to-table GameTx

Validation Behavior

Before final load, the script validates:

file_jsonl_line_count == copied_to_temp_count == temp_table_count

If not matched, the script prints:

status : FAIL

and aborts the final insert.

Common Errors and Fixes
Error: password authentication failed

Check if the password variable is empty or contains a newline:

printf '%q\n' "$PG_PWD"
printf '%s' "$PG_PWD" | wc -c

Error: no pg_hba.conf entry, no encryption
Use SSL mode in the PostgreSQL connection. The script should connect with:

sslmode="require"

Error: missing privilege SELECT, INSERT
Grant required privileges on the final table:

GRANT SELECT, INSERT
ON TABLE public."GameTransactionInplayV1"
TO egms_loader;

Error: required table does not exist
For mixed-case PostgreSQL table names, pass the raw name without shell quotes:
--to-table GameTransactionInplayV1

The script resolves and quotes the table name internally.

Recommended Run Order

Recommended production-safe sequence:

1. Run extractor.
2. Confirm extractor status is PASS.
3. Run loader with --check-only.
4. Run loader without --final for temp validation.
5. Run loader with --final for actual migration.
6. Review final inserted/skipped/history counts.
7. Review load_skip_out if skipped records exist.

