#!/usr/bin/env python3
"""Container scheduler for the Elasticsearch cold-data archive job."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo


LOG = logging.getLogger("elk-cold-archive-scheduler")
STOP = threading.Event()


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {value!r}")


def parse_schedule_time(value: str) -> dt.time:
    try:
        hour_text, minute_text = value.split(":", 1)
        return dt.time(hour=int(hour_text), minute=int(minute_text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"SCHEDULE_TIME must use HH:MM, got {value!r}") from exc


def initial_last_schedule_date(now: dt.datetime, schedule_time: dt.time) -> dt.date:
    """Skip an already-passed schedule only on the very first container start."""
    if now.timetz().replace(tzinfo=None) >= schedule_time:
        return now.date()
    return now.date() - dt.timedelta(days=1)


def due_schedule_date(
    now: dt.datetime, last_schedule_date: dt.date, schedule_time: dt.time
) -> dt.date | None:
    if last_schedule_date >= now.date():
        return None
    if now.timetz().replace(tzinfo=None) >= schedule_time:
        return now.date()
    return None


def next_schedule_at(
    now: dt.datetime, last_schedule_date: dt.date, schedule_time: dt.time
) -> dt.datetime:
    if last_schedule_date < now.date() and now.timetz().replace(tzinfo=None) < schedule_time:
        target_date = now.date()
    else:
        target_date = now.date() + dt.timedelta(days=1)
    return dt.datetime.combine(target_date, schedule_time, tzinfo=now.tzinfo)


def reconcile_schedule_state(
    state: dict[str, Any], now: dt.datetime, schedule_time: dt.time, mode: str
) -> dt.date:
    last_text = str(state.get("last_schedule_date", ""))
    if last_text:
        try:
            last_schedule_date = dt.date.fromisoformat(last_text)
        except ValueError as exc:
            raise RuntimeError(f"Invalid last_schedule_date: {last_text!r}") from exc
    else:
        last_schedule_date = initial_last_schedule_date(now, schedule_time)
        state["initialized_at"] = now.isoformat()

    configured_mode = str(state.get("configured_mode", ""))
    if configured_mode and configured_mode != mode:
        # A dry-run/live transition never consumes an already-passed slot implicitly.
        last_schedule_date = initial_last_schedule_date(now, schedule_time)
        state["mode_changed_at"] = now.isoformat()
    state["configured_mode"] = mode
    state["last_schedule_date"] = last_schedule_date.isoformat()
    return last_schedule_date


class JsonState:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot read scheduler state {self.path}: {exc}") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"Scheduler state {self.path} must contain a JSON object")
        return value

    def save(self, value: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(prefix=".scheduler.", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(value, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_name, 0o600)
            os.replace(temp_name, self.path)
        finally:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass


def heartbeat(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    os.utime(path, None)


def interruptible_wait(seconds: float, heartbeat_path: pathlib.Path) -> bool:
    deadline = time.monotonic() + max(0, seconds)
    while not STOP.is_set():
        heartbeat(heartbeat_path)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        STOP.wait(min(remaining, 30))
    return False


def archive_command(execute: bool, no_mail: bool = False) -> list[str]:
    command = [
        sys.executable,
        os.environ.get("ARCHIVE_SCRIPT", "/app/archive.py"),
        "--config",
        os.environ.get("ARCHIVE_CONFIG", "/config/config.json"),
    ]
    if execute:
        command.append("--execute")
    if no_mail:
        command.append("--no-mail")
    return command


def run_archive(execute: bool, no_mail: bool, heartbeat_path: pathlib.Path) -> int:
    command = archive_command(execute=execute, no_mail=no_mail)
    LOG.info("Starting archive job in %s mode", "LIVE" if execute else "DRY-RUN")
    process = subprocess.Popen(command, start_new_session=True)
    while process.poll() is None:
        heartbeat(heartbeat_path)
        if STOP.wait(5):
            LOG.warning("Stopping archive child process")
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except (AttributeError, ProcessLookupError):
                process.terminate()
            try:
                return process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except (AttributeError, ProcessLookupError):
                    process.kill()
                return process.wait(timeout=10)
    return int(process.returncode or 0)


def run_with_retries(
    execute: bool,
    no_mail: bool,
    heartbeat_path: pathlib.Path,
    max_retries: int,
    retry_seconds: int,
    due_date: dt.date,
    timezone: ZoneInfo,
    store: JsonState,
    state: dict[str, Any],
) -> int:
    pending_date = str(state.get("pending_schedule_date", ""))
    attempt = int(state.get("pending_attempt", 0)) if pending_date == due_date.isoformat() else 0
    retry_not_before = str(state.get("retry_not_before", ""))
    if pending_date == due_date.isoformat() and retry_not_before:
        try:
            retry_at = dt.datetime.fromisoformat(retry_not_before)
        except ValueError as exc:
            raise RuntimeError(f"Invalid retry_not_before: {retry_not_before!r}") from exc
        delay = (retry_at - dt.datetime.now(timezone)).total_seconds()
        if delay > 0 and not interruptible_wait(delay, heartbeat_path):
            return 1

    while not STOP.is_set():
        state.update(
            {
                "pending_schedule_date": due_date.isoformat(),
                "pending_attempt": attempt,
                "retry_not_before": "",
            }
        )
        store.save(state)
        exit_code = run_archive(execute, no_mail, heartbeat_path)
        if STOP.is_set():
            return exit_code
        if exit_code in {0, 2, 3} or attempt >= max_retries:
            state.pop("pending_schedule_date", None)
            state.pop("pending_attempt", None)
            state.pop("retry_not_before", None)
            store.save(state)
            return exit_code
        attempt += 1
        retry_at = dt.datetime.now(timezone) + dt.timedelta(seconds=retry_seconds)
        state.update(
            {
                "pending_schedule_date": due_date.isoformat(),
                "pending_attempt": attempt,
                "retry_not_before": retry_at.isoformat(),
            }
        )
        store.save(state)
        LOG.warning(
            "Archive job exited %d; retry %d/%d in %d seconds",
            exit_code,
            attempt,
            max_retries,
            retry_seconds,
        )
        if not interruptible_wait(retry_seconds, heartbeat_path):
            break
    return 1


def healthcheck(path: pathlib.Path, max_age_seconds: int) -> int:
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return 1
    return 0 if -30 <= age <= max_age_seconds else 1


def scheduler_loop() -> int:
    timezone = ZoneInfo(os.environ.get("SCHEDULE_TIMEZONE", "Asia/Shanghai"))
    schedule_time = parse_schedule_time(os.environ.get("SCHEDULE_TIME", "02:00"))
    state_path = pathlib.Path(os.environ.get("SCHEDULER_STATE_FILE", "/state/scheduler.json"))
    heartbeat_path = pathlib.Path(os.environ.get("HEARTBEAT_FILE", "/state/heartbeat"))
    execute = env_bool("ARCHIVE_EXECUTE", False)
    mode = "LIVE" if execute else "DRY-RUN"
    max_retries = max(0, int(os.environ.get("MAX_RETRIES", "2")))
    retry_seconds = max(1, int(os.environ.get("RETRY_MINUTES", "30"))) * 60
    store = JsonState(state_path)
    state = store.load()

    now = dt.datetime.now(timezone)
    last_schedule_date = reconcile_schedule_state(state, now, schedule_time, mode)
    store.save(state)

    LOG.info(
        "Scheduler ready: mode=%s schedule=%s %s last_schedule_date=%s",
        mode,
        schedule_time.strftime("%H:%M"),
        timezone.key,
        last_schedule_date,
    )
    while not STOP.is_set():
        heartbeat(heartbeat_path)
        now = dt.datetime.now(timezone)
        due_date = due_schedule_date(now, last_schedule_date, schedule_time)
        if due_date is not None:
            exit_code = run_with_retries(
                execute=execute,
                no_mail=False,
                heartbeat_path=heartbeat_path,
                max_retries=max_retries,
                retry_seconds=retry_seconds,
                due_date=due_date,
                timezone=timezone,
                store=store,
                state=state,
            )
            if STOP.is_set():
                break
            last_schedule_date = due_date
            state.update(
                {
                    "last_schedule_date": due_date.isoformat(),
                    "last_exit_code": exit_code,
                    "last_finished_at": dt.datetime.now(timezone).isoformat(),
                    "mode": mode,
                }
            )
            store.save(state)
            LOG.info("Scheduled archive finished with exit code %d", exit_code)
            continue

        target = next_schedule_at(now, last_schedule_date, schedule_time)
        wait_seconds = max(1, (target - now).total_seconds())
        LOG.info("Next archive run at %s", target.isoformat())
        interruptible_wait(wait_seconds, heartbeat_path)
    LOG.info("Scheduler stopped")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one archive job and exit")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Force one-shot dry-run mode")
    mode.add_argument("--execute", action="store_true", help="Force one-shot live mode")
    parser.add_argument("--no-mail", action="store_true", help="Pass --no-mail to a dry-run")
    parser.add_argument("--healthcheck", action="store_true")
    return parser.parse_args(argv)


def handle_signal(signum: int, frame: Any) -> None:
    del frame
    LOG.warning("Received signal %d", signum)
    STOP.set()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.healthcheck:
        path = pathlib.Path(os.environ.get("HEARTBEAT_FILE", "/state/heartbeat"))
        max_age = int(os.environ.get("HEALTH_MAX_AGE_SECONDS", "180"))
        return healthcheck(path, max_age)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    heartbeat_path = pathlib.Path(os.environ.get("HEARTBEAT_FILE", "/state/heartbeat"))
    if args.once:
        execute = args.execute or (env_bool("ARCHIVE_EXECUTE", False) and not args.dry_run)
        if execute and args.no_mail:
            raise ValueError("--no-mail cannot be combined with live mode")
        return run_archive(execute=execute, no_mail=args.no_mail, heartbeat_path=heartbeat_path)
    if args.dry_run or args.execute or args.no_mail:
        raise ValueError("--dry-run, --execute, and --no-mail require --once")
    return scheduler_loop()


if __name__ == "__main__":
    raise SystemExit(main())
