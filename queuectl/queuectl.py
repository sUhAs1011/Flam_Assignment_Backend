#!/usr/bin/env python3
"""
QueueCTL — a CLI-based background job queue system (single-file version)

Features
- Enqueue jobs with JSON spec
- Multiple workers with graceful shutdown
- Persistent storage in SQLite (jobs, dlq, config)
- Exponential backoff retries (delay = base ** attempts)
- Dead Letter Queue (DLQ) after max_retries
- Status, list, DLQ operations, config management
- Minimal dependencies (standard library only)

Tested with Python 3.10+.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

APP_NAME = "queuectl"
DEFAULT_DB = os.environ.get("QUEUECTL_DB", os.path.join(os.getcwd(), "queuectl.db"))
STATE_DIR = Path(os.environ.get("QUEUECTL_STATE", os.path.join(os.getcwd(), ".queuectl")))
PIDS_DIR = STATE_DIR / "pids"
LOGS_DIR = STATE_DIR / "logs"

# --- graceful shutdown flag ---
_STOP_REQUESTED = threading.Event()


def utcnow_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def parse_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


# --------------------- DB LAYER ---------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    run_at TEXT NOT NULL,
    last_error TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    timeout INTEGER,
    worker_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_state_runat ON jobs(state, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority, created_at);

CREATE TABLE IF NOT EXISTS dlq (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    failed_at TEXT NOT NULL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

DEFAULT_CONFIG = {
    "max_retries": "3",
    "backoff_base": "2",
    "job_timeout": "300"  # seconds (bonus feature)
}


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    # Retry init a few times in case multiple workers race on startup
    for i in range(8):
        try:
            with conn:
                conn.executescript(SCHEMA_SQL)
                for k, v in DEFAULT_CONFIG.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", (k, v)
                    )
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.1 * (i + 1))
                continue
            raise
    return conn


def cfg_get(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> str:
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    return row[0] if row else (default if default is not None else "")


def cfg_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    with conn:
        conn.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


# --------------------- JOB OPERATIONS ---------------------

def enqueue_job(conn: sqlite3.Connection, job: Dict[str, Any]) -> None:
    now = utcnow_iso()
    # defaults
    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    job.setdefault("max_retries", int(cfg_get(conn, "max_retries", "3")))
    job.setdefault("created_at", now)
    job.setdefault("updated_at", now)
    # support optional priority, timeout, run_at
    priority = int(job.get("priority", 100))
    timeout = job.get("timeout")
    run_at = job.get("run_at", now)
    with conn:
        conn.execute(
            """
            INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, run_at, last_error, priority, timeout, worker_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)
            """,
            (
                job["id"], job["command"], job["state"], int(job["attempts"]),
                int(job["max_retries"]), job["created_at"], job["updated_at"], run_at, job.get("last_error"),
                priority, int(timeout) if timeout is not None else None,
            ),
        )


def next_job_claim(conn: sqlite3.Connection, worker_id: str) -> Optional[sqlite3.Row]:
    now = utcnow_iso()
    # Use an IMMEDIATE transaction to avoid races between workers
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT * FROM jobs WHERE state='pending' AND run_at<=? ORDER BY priority ASC, created_at ASC LIMIT 1",
        (now,),
    ).fetchone()
    if not row:
        conn.execute("COMMIT")
        return None
    # attempt to mark as processing (double-check state)
    updated = conn.execute(
        "UPDATE jobs SET state='processing', worker_id=?, updated_at=? WHERE id=? AND state='pending'",
        (worker_id, now, row["id"]),
    )
    conn.execute("COMMIT")
    if updated.rowcount == 1:
        return row
    return None


def complete_job(conn: sqlite3.Connection, job_id: str) -> None:
    with conn:
        conn.execute(
            "UPDATE jobs SET state='completed', updated_at=?, worker_id=NULL WHERE id=?",
            (utcnow_iso(), job_id),
        )


def fail_or_retry_job(conn: sqlite3.Connection, row: sqlite3.Row, error: str) -> None:
    attempts = int(row["attempts"]) + 1
    max_retries = int(row["max_retries"])
    base = int(cfg_get(conn, "backoff_base", "2"))
    now = utcnow_iso()

    if attempts > max_retries:
        # move to DLQ
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO dlq(id, command, attempts, max_retries, failed_at, last_error) VALUES(?,?,?,?,?,?)",
                (row["id"], row["command"], attempts - 1, max_retries, now, error[:8000]),
            )
            conn.execute("DELETE FROM jobs WHERE id=?", (row["id"],))
        return

    delay = base ** attempts
    run_at = (parse_iso(now) + dt.timedelta(seconds=delay)).isoformat()

    with conn:
        conn.execute(
            """
            UPDATE jobs
            SET state='pending', attempts=?, run_at=?, last_error=?, updated_at=?, worker_id=NULL
            WHERE id=?
            """,
            (attempts, run_at, error[:8000], now, row["id"]),
        )


# --------------------- WORKER ---------------------

def ensure_dirs():
    PIDS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def install_signal_handlers():
    def _handler(signum, frame):
        _STOP_REQUESTED.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handler)


def run_command(command: str, timeout: Optional[int], job_id: str) -> Tuple[int, str, str]:
    """Run the shell command, return (exit_code, stdout, stderr)."""
    log_path = LOGS_DIR / f"{job_id}.log"
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout if timeout and timeout > 0 else None,
        )
        stdout, stderr = proc.stdout or "", proc.stderr or ""
        with open(log_path, "a", encoding="utf-8") as f:
            now = utcnow_iso()
            f.write(f"[{now}] EXIT={proc.returncode}\nSTDOUT\n{stdout}\nSTDERR\n{stderr}\n\n")
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired as e:
        with open(log_path, "a", encoding="utf-8") as f:
            now = utcnow_iso()
            f.write(f"[{now}] TIMEOUT after {timeout}s for command: {command}\n")
        return 124, "", f"timeout after {timeout}s"


def worker_loop(db_path: str, worker_id: str, poll_interval: float = 1.0) -> None:
    ensure_dirs()
    install_signal_handlers()
    conn = db_connect(db_path)
    while not _STOP_REQUESTED.is_set():
        row = next_job_claim(conn, worker_id)
        if not row:
            time.sleep(poll_interval)
            continue
        # Execute the job
        timeout_cfg = cfg_get(conn, "job_timeout", "300")
        timeout = int(row["timeout"]) if row["timeout"] is not None else int(timeout_cfg)
        rc, _out, err = run_command(row["command"], timeout, row["id"]) 
        if rc == 0:
            complete_job(conn, row["id"])
        else:
            fail_or_retry_job(conn, row, err or f"exit code {rc}")
    # finish current loop iteration and exit


# --------------------- CLI ---------------------

def cmd_enqueue(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    try:
        if args.file:
            payload = json.load(open(args.file, "r", encoding="utf-8"))
        else:
            payload = json.loads(args.json)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        sys.exit(2)

    if not isinstance(payload, dict):
        print("Job must be a JSON object", file=sys.stderr)
        sys.exit(2)

    required = ["id", "command"]
    for k in required:
        if k not in payload:
            print(f"Missing required field: {k}", file=sys.stderr)
            sys.exit(2)

    enqueue_job(conn, payload)
    print(f"Enqueued job {payload['id']}")


def _spawn_worker(db: str, idx: int) -> int:
    pid = os.fork()
    if pid == 0:
        # child
        wid = f"{os.getpid()}"
        # write pidfile
        ensure_dirs()
        with open(PIDS_DIR / f"worker.{wid}.pid", "w") as f:
            f.write(wid)
        try:
            worker_loop(db, worker_id=wid)
        finally:
            # cleanup pidfile
            try:
                (PIDS_DIR / f"worker.{wid}.pid").unlink(missing_ok=True)
            except Exception:
                pass
        os._exit(0)
    return pid


def cmd_worker_start(args: argparse.Namespace) -> None:
    ensure_dirs()
    # Ensure DB schema is created before forking workers
    c = db_connect(args.db)
    c.close()
    count = int(args.count)
    pids = []
    for i in range(count):
        pid = _spawn_worker(args.db, i)
        pids.append(pid)
        time.sleep(0.05)  # small stagger to avoid startup contention
    print(f"Started {len(pids)} worker(s): {', '.join(map(str,pids))}")


def _iter_worker_pids():
    if not PIDS_DIR.exists():
        return []
    pids = []
    for f in PIDS_DIR.glob("worker.*.pid"):
        try:
            p = int(f.read_text().strip())
            pids.append(p)
        except Exception:
            pass
    return pids


def cmd_worker_stop(_args: argparse.Namespace) -> None:
    pids = _iter_worker_pids()
    if not pids:
        print("No worker PIDs found.")
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    print(f"Signaled {len(pids)} worker(s) to stop. They will finish the current job then exit.")


def cmd_status(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    counts = {}
    for state in ("pending", "processing", "completed"):
        counts[state] = conn.execute("SELECT COUNT(*) FROM jobs WHERE state=?", (state,)).fetchone()[0]
    dlq_count = conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    workers = [pid for pid in _iter_worker_pids() if _proc_alive(pid)]
    print("Jobs:")
    for k, v in counts.items():
        print(f"  {k:>11}: {v}")
    print(f"  {'in_dlq':>11}: {dlq_count}")
    print(f"Workers active: {len(workers)} -> {', '.join(map(str, workers)) if workers else '-'}")


def _proc_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def cmd_list(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    q = "SELECT id, state, attempts, max_retries, priority, run_at, updated_at, command FROM jobs"
    params = []
    if args.state:
        q += " WHERE state=?"
        params.append(args.state)
    q += " ORDER BY created_at ASC"
    rows = conn.execute(q, tuple(params)).fetchall()
    if not rows:
        print("No jobs.")
        return
    for r in rows:
        print(f"{r['id']}: state={r['state']}, attempts={r['attempts']}/{r['max_retries']}, run_at={r['run_at']}, prio={r['priority']}\n    cmd={r['command']}")


def cmd_dlq_list(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    rows = conn.execute("SELECT * FROM dlq ORDER BY failed_at DESC").fetchall()
    if not rows:
        print("DLQ is empty.")
        return
    for r in rows:
        print(f"{r['id']}: attempts={r['attempts']}/{r['max_retries']}, failed_at={r['failed_at']}\n    last_error={r['last_error']}")


def cmd_dlq_retry(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    row = conn.execute("SELECT * FROM dlq WHERE id=?", (args.job_id,)).fetchone()
    if not row:
        print(f"Job {args.job_id} not found in DLQ", file=sys.stderr)
        sys.exit(1)
    now = utcnow_iso()
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, run_at, last_error, priority, timeout, worker_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)",
            (
                row["id"], row["command"], "pending", 0, row["max_retries"], now, now, now, None, 100, None,
            ),
        )
        conn.execute("DELETE FROM dlq WHERE id=?", (row["id"],))
    print(f"Re-enqueued {row['id']} from DLQ")


def cmd_config_get(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    if args.key:
        print(cfg_get(conn, args.key, ""))
    else:
        rows = conn.execute("SELECT key,value FROM config ORDER BY key").fetchall()
        for r in rows:
            print(f"{r['key']}={r['value']}")


def cmd_config_set(args: argparse.Namespace) -> None:
    conn = db_connect(args.db)
    cfg_set(conn, args.key, args.value)
    print(f"set {args.key}={args.value}")


# --------------------- ARGPARSE SETUP ---------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=APP_NAME, description="CLI background job queue system")
    p.add_argument("--db", default=DEFAULT_DB, help=f"Path to sqlite db (default: {DEFAULT_DB})")
    sub = p.add_subparsers(dest="cmd", required=True)

    # enqueue
    sp = sub.add_parser("enqueue", help="Add a new job to the queue")
    g = sp.add_mutually_exclusive_group(required=True)
    g.add_argument("--json", help="Job JSON inline string")
    g.add_argument("--file", help="Path to JSON file with job spec")
    sp.set_defaults(func=cmd_enqueue)

    # worker
    spw = sub.add_parser("worker", help="Worker operations")
    subw = spw.add_subparsers(dest="wcmd", required=True)

    spws = subw.add_parser("start", help="Start one or more workers")
    spws.add_argument("--count", type=int, default=1, help="Number of workers to start")
    spws.set_defaults(func=cmd_worker_start)

    spwx = subw.add_parser("stop", help="Stop running workers gracefully")
    spwx.set_defaults(func=cmd_worker_stop)

    # status
    sps = sub.add_parser("status", help="Show summary of all job states and active workers")
    sps.set_defaults(func=cmd_status)

    # list
    spl = sub.add_parser("list", help="List jobs by state")
    spl.add_argument("--state", choices=["pending","processing","completed"], help="Filter by state")
    spl.set_defaults(func=cmd_list)

    # dlq
    spd = sub.add_parser("dlq", help="Dead Letter Queue operations")
    subd = spd.add_subparsers(dest="dcmd", required=True)

    spdl = subd.add_parser("list", help="List DLQ jobs")
    spdl.set_defaults(func=cmd_dlq_list)

    spdr = subd.add_parser("retry", help="Retry a DLQ job by id")
    spdr.add_argument("job_id")
    spdr.set_defaults(func=cmd_dlq_retry)

    # config
    spc = sub.add_parser("config", help="Configuration management")
    subc = spc.add_subparsers(dest="ccmd", required=True)

    spcg = subc.add_parser("get", help="Get config or all if no key provided")
    spcg.add_argument("key", nargs="?")
    spcg.set_defaults(func=cmd_config_get)

    spcs = subc.add_parser("set", help="Set a configuration value")
    spcs.add_argument("key")
    spcs.add_argument("value")
    spcs.set_defaults(func=cmd_config_set)

    return p


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    # dispatch
    args.func(args)


if __name__ == "__main__":
    main()
