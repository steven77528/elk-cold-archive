import datetime as dt
import importlib.util
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("scheduler.py")
SPEC = importlib.util.spec_from_file_location("elk_cold_scheduler", MODULE_PATH)
scheduler = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = scheduler
SPEC.loader.exec_module(scheduler)


TZ = dt.timezone(dt.timedelta(hours=8))
SCHEDULE = dt.time(2, 0)


class ScheduleTests(unittest.TestCase):
    def test_first_start_after_schedule_skips_immediate_live_run(self):
        now = dt.datetime(2026, 7, 14, 10, 0, tzinfo=TZ)
        last = scheduler.initial_last_schedule_date(now, SCHEDULE)
        self.assertEqual(dt.date(2026, 7, 14), last)
        self.assertIsNone(scheduler.due_schedule_date(now, last, SCHEDULE))

    def test_first_start_before_schedule_runs_today(self):
        now = dt.datetime(2026, 7, 14, 1, 0, tzinfo=TZ)
        last = scheduler.initial_last_schedule_date(now, SCHEDULE)
        self.assertEqual(dt.date(2026, 7, 13), last)
        target = scheduler.next_schedule_at(now, last, SCHEDULE)
        self.assertEqual(dt.datetime(2026, 7, 14, 2, 0, tzinfo=TZ), target)

    def test_restart_after_missed_schedule_catches_up(self):
        now = dt.datetime(2026, 7, 15, 8, 0, tzinfo=TZ)
        due = scheduler.due_schedule_date(now, dt.date(2026, 7, 14), SCHEDULE)
        self.assertEqual(dt.date(2026, 7, 15), due)

    def test_restart_before_schedule_waits(self):
        now = dt.datetime(2026, 7, 15, 1, 30, tzinfo=TZ)
        last = dt.date(2026, 7, 14)
        self.assertIsNone(scheduler.due_schedule_date(now, last, SCHEDULE))
        self.assertEqual(
            dt.datetime(2026, 7, 15, 2, 0, tzinfo=TZ),
            scheduler.next_schedule_at(now, last, SCHEDULE),
        )

    def test_archive_command_never_adds_execute_for_dry_run(self):
        command = scheduler.archive_command(execute=False, no_mail=True)
        self.assertNotIn("--execute", command)
        self.assertIn("--no-mail", command)

    def test_mode_change_after_schedule_does_not_catch_up_live(self):
        now = dt.datetime(2026, 7, 15, 10, 0, tzinfo=TZ)
        state = {
            "last_schedule_date": "2026-07-14",
            "configured_mode": "DRY-RUN",
        }
        last = scheduler.reconcile_schedule_state(state, now, SCHEDULE, "LIVE")
        self.assertEqual(dt.date(2026, 7, 15), last)
        self.assertIsNone(scheduler.due_schedule_date(now, last, SCHEDULE))
        self.assertEqual("LIVE", state["configured_mode"])

    def test_healthcheck_rejects_future_heartbeat(self):
        import os
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "heartbeat"
            path.touch()
            future = time.time() + 120
            os.utime(path, (future, future))
            self.assertEqual(1, scheduler.healthcheck(path, 180))


if __name__ == "__main__":
    unittest.main()
