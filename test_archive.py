import datetime as dt
import importlib.util
import pathlib
import sys
import tempfile
import unittest
import urllib.parse


MODULE_PATH = pathlib.Path(__file__).with_name("archive.py")
SPEC = importlib.util.spec_from_file_location("elk_cold_archive", MODULE_PATH)
archive = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = archive
SPEC.loader.exec_module(archive)


def index(name, uuid):
    return {"index_name": name, "index_uuid": uuid}


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.streams = [
            {
                "name": "logs-gateway-prod",
                "indices": [
                    index("gateway-age-31", "u31"),
                    index("gateway-age-30", "u30"),
                    index("gateway-age-7", "u7"),
                    index("gateway-age-6", "u6"),
                    index("gateway-write", "uw"),
                ],
            }
        ]
        self.config = {
            "timezone": "Asia/Shanghai",
            "rules": [
                {
                    "name": "gateway",
                    "data_stream_patterns": ["logs-gateway-*"],
                    "min_age_days": 7,
                    "max_age_days": 30,
                }
            ],
            "max_indices_per_run": 100,
            "max_data_streams_per_snapshot": 10,
        }
        shanghai_now = dt.datetime(2026, 7, 14, 2, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))
        self.now = shanghai_now.astimezone(dt.timezone.utc)

    def millis_for_age(self, days):
        value = self.now.astimezone(dt.timezone(dt.timedelta(hours=8))) - dt.timedelta(days=days)
        return int(value.timestamp() * 1000)

    def test_inclusive_age_window_and_write_index_exclusion(self):
        times = {
            "gateway-age-31": self.millis_for_age(31),
            "gateway-age-30": self.millis_for_age(30),
            "gateway-age-7": self.millis_for_age(7),
            "gateway-age-6": self.millis_for_age(6),
        }
        result = archive.select_candidates(self.streams, times, self.config, self.now)
        self.assertEqual(["gateway-age-30", "gateway-age-7"], [item.index for item in result.candidates])
        self.assertEqual(["gateway-age-31"], [item.index for item in result.overdue])
        self.assertNotIn("gateway-write", [item.index for item in result.candidates])

    def test_overlapping_rules_fail_closed(self):
        self.config["rules"].append(
            {
                "name": "overlap",
                "data_stream_patterns": ["logs-*-prod"],
                "min_age_days": 1,
                "max_age_days": 2,
            }
        )
        with self.assertRaises(archive.ArchiveError):
            archive.select_candidates(self.streams, {}, self.config, self.now)


class RolloverOverrideSelectionTests(unittest.TestCase):
    OLD_INDEX = ".ds-logs-gateway-prod-2026.08.04-000001"
    WRITE_INDEX = ".ds-logs-gateway-prod-2026.08.31-000002"
    OVERRIDE_TIME_MS = 1786605291333

    def setUp(self):
        self.now = dt.datetime(2026, 8, 31, 10, 0, tzinfo=dt.timezone.utc)
        self.selection = {
            "timezone": "Asia/Shanghai",
            "rules": [
                {
                    "name": "gateway",
                    "data_stream_patterns": ["logs-gateway-*"],
                    "min_age_days": 7,
                    "max_age_days": 30,
                }
            ],
            "max_indices_per_run": 100,
            "max_data_streams_per_snapshot": 10,
            "rollover_time_overrides": [
                {
                    "index": self.OLD_INDEX,
                    "index_uuid": "old-uuid",
                    "rollover_time_ms": self.OVERRIDE_TIME_MS,
                    "reason": "Restored backing index has no rollover_info",
                }
            ],
        }

    def client(
        self,
        rollover_info=None,
        old_uuid="old-uuid",
        include_rollover_info=True,
    ):
        old_index = self.OLD_INDEX
        write_index = self.WRITE_INDEX

        class SelectionClient:
            metadata_path = ""

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                if method == "GET" and path.startswith("/_data_stream"):
                    return 200, {
                        "data_streams": [
                            {
                                "name": "logs-gateway-prod",
                                "indices": [
                                    index(old_index, old_uuid),
                                    index(write_index, "write-uuid"),
                                ],
                            }
                        ]
                    }
                if method == "GET" and path.startswith("/_cluster/state/metadata/"):
                    self.metadata_path = path
                    metadata_entry = {}
                    if include_rollover_info:
                        metadata_entry["rollover_info"] = rollover_info or {}
                    return 200, {
                        "metadata": {
                            "indices": {
                                old_index: metadata_entry,
                            }
                        }
                    }
                raise AssertionError(f"Unexpected request: {method} {path}")

        return SelectionClient()

    def test_missing_rollover_info_uses_exact_reviewed_override(self):
        client = self.client()
        result = archive.discover_selection(client, self.selection, self.now)

        self.assertEqual([self.OLD_INDEX], [item.index for item in result.candidates])
        metadata_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(client.metadata_path).query
        )
        self.assertEqual(
            ["metadata.indices.*.rollover_info"],
            metadata_query["filter_path"],
        )
        candidate = result.candidates[0]
        self.assertEqual(self.OVERRIDE_TIME_MS, candidate.rollover_time_ms)
        self.assertEqual("override", candidate.rollover_time_source)
        self.assertEqual(
            "Restored backing index has no rollover_info",
            candidate.rollover_override_reason,
        )
        self.assertTrue(any(self.OLD_INDEX in warning for warning in result.warnings))
        transaction = archive.create_transaction(
            "repo", "snapshot", [candidate], "cluster-uuid"
        )
        self.assertEqual("override", transaction["indices"][0]["rollover_time_source"])
        self.assertEqual(
            "Restored backing index has no rollover_info",
            transaction["indices"][0]["rollover_override_reason"],
        )

    def test_override_uuid_must_match_current_backing_index(self):
        with self.assertRaisesRegex(archive.ArchiveError, "UUID mismatch"):
            archive.discover_selection(
                self.client(old_uuid="recreated-uuid"), self.selection, self.now
            )

    def test_override_cannot_replace_authoritative_rollover_info(self):
        rollover_info = {
            "logs-gateway-prod": {"time": self.OVERRIDE_TIME_MS - 1000}
        }
        with self.assertRaisesRegex(archive.ArchiveError, "authoritative rollover_info"):
            archive.discover_selection(
                self.client(rollover_info), self.selection, self.now
            )

    def test_override_cannot_mask_invalid_rollover_time(self):
        rollover_info = {"logs-gateway-prod": {"time": "invalid"}}
        with self.assertRaisesRegex(archive.ArchiveError, "invalid rollover time"):
            archive.discover_selection(
                self.client(rollover_info), self.selection, self.now
            )

    def test_override_cannot_mask_rollover_owner_without_time(self):
        rollover_info = {"logs-gateway-prod": {}}
        with self.assertRaisesRegex(archive.ArchiveError, "invalid rollover time"):
            archive.discover_selection(
                self.client(rollover_info), self.selection, self.now
            )

    def test_override_cannot_mask_different_rollover_owner(self):
        rollover_info = {"old-data-stream": {"time": self.OVERRIDE_TIME_MS}}
        with self.assertRaisesRegex(archive.ArchiveError, "different owner"):
            archive.discover_selection(
                self.client(rollover_info), self.selection, self.now
            )

    def test_missing_rollover_info_without_override_still_fails(self):
        self.selection["rollover_time_overrides"] = []
        with self.assertRaisesRegex(archive.ArchiveError, "has no rollover_info"):
            archive.discover_selection(self.client(), self.selection, self.now)

    def test_override_requires_explicit_empty_rollover_info(self):
        with self.assertRaisesRegex(archive.ArchiveError, "no rollover_info metadata"):
            archive.discover_selection(
                self.client(include_rollover_info=False), self.selection, self.now
            )

    def test_malformed_cluster_state_metadata_fails_with_archive_error(self):
        data_stream_client = self.client()

        class MalformedMetadataClient:
            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                if method == "GET" and path.startswith("/_cluster/state/metadata/"):
                    return 200, {"metadata": {"indices": []}}
                return data_stream_client.request(
                    method, path, body, allowed_status, timeout
                )

        with self.assertRaisesRegex(archive.ArchiveError, "invalid metadata"):
            archive.discover_selection(
                MalformedMetadataClient(), self.selection, self.now
            )

    def test_unused_override_fails_the_entire_discovery(self):
        self.selection["rollover_time_overrides"][0]["index"] = ".ds-logs-gateway-typo"
        native_time = {"logs-gateway-prod": {"time": self.OVERRIDE_TIME_MS}}
        with self.assertRaisesRegex(archive.ArchiveError, "did not match"):
            archive.discover_selection(
                self.client(native_time), self.selection, self.now
            )

    def test_write_index_override_is_rejected_as_unused(self):
        override = self.selection["rollover_time_overrides"][0]
        override["index"] = self.WRITE_INDEX
        override["index_uuid"] = "write-uuid"
        native_time = {"logs-gateway-prod": {"time": self.OVERRIDE_TIME_MS}}
        with self.assertRaisesRegex(archive.ArchiveError, "non-write backing index"):
            archive.discover_selection(
                self.client(native_time), self.selection, self.now
            )

    def test_future_override_time_is_never_selected(self):
        future = self.now + dt.timedelta(days=1)
        self.selection["rollover_time_overrides"][0]["rollover_time_ms"] = int(
            future.timestamp() * 1000
        )
        with self.assertRaisesRegex(archive.ArchiveError, "future rollover date"):
            archive.discover_selection(self.client(), self.selection, self.now)

    def test_same_day_future_override_time_is_never_selected(self):
        future = self.now + dt.timedelta(hours=1)
        self.selection["rollover_time_overrides"][0]["rollover_time_ms"] = int(
            future.timestamp() * 1000
        )
        with self.assertRaisesRegex(archive.ArchiveError, "future rollover date"):
            archive.discover_selection(self.client(), self.selection, self.now)


class SnapshotTests(unittest.TestCase):
    def test_success_requires_candidate_coverage_stream_set_and_all_shards(self):
        snapshot = {
            "state": "SUCCESS",
            "indices": ["a", "b"],
            "data_streams": ["stream-a"],
            "shards": {"total": 4, "successful": 4, "failed": 0},
            "failures": [],
        }
        archive.validate_successful_snapshot(snapshot, ["b", "a"], ["stream-a"])
        snapshot["indices"] = ["a"]
        with self.assertRaises(archive.ArchiveError):
            archive.validate_successful_snapshot(snapshot, ["a", "b"], ["stream-a"])

    def test_partial_snapshot_is_never_accepted(self):
        snapshot = {
            "state": "PARTIAL",
            "indices": ["a"],
            "data_streams": ["stream-a"],
            "shards": {"total": 2, "successful": 1, "failed": 1},
            "failures": [{"index": "a"}],
        }
        with self.assertRaises(archive.ArchiveError):
            archive.validate_successful_snapshot(snapshot, ["a"], ["stream-a"])

    def test_snapshot_request_uses_data_stream_not_backing_index(self):
        class CaptureClient:
            def __init__(self):
                self.body = None

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.body = body
                return 200, {"accepted": True}

        candidate = archive.Candidate(
            index=".ds-logs-gateway-prod-2026.07.01-000001",
            index_uuid="uuid-1",
            data_stream="logs-gateway-prod",
            rule="gateway",
            rollover_time_ms=1,
            rollover_local="2026-07-01T00:00:00+08:00",
            age_days=7,
        )
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            store = archive.StateStore(base / "state.json")
            audit = archive.AuditLog(base / "audit.jsonl")
            state = archive.create_transaction("repo", "snapshot", [candidate], "cluster-uuid")
            state["retention_days"] = 23
            client = CaptureClient()
            archive.start_snapshot(client, store, audit, state)
        self.assertEqual("logs-gateway-prod", client.body["indices"])
        self.assertFalse(client.body["partial"])
        self.assertNotIn(candidate.index, client.body["indices"])
        self.assertEqual(23, client.body["metadata"]["retention_days"])


class SnapshotCleanupPlanningTests(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026, 8, 10, 0, 0, tzinfo=dt.timezone.utc)
        self.rules = [
            {
                "name": "gateway-7-to-30-days",
                "data_stream_patterns": ["logs-gateway-*"],
                "min_age_days": 7,
                "max_age_days": 30,
            },
            {
                "name": "nginx-15-to-30-days",
                "data_stream_patterns": ["logs-nginx-*"],
                "min_age_days": 15,
                "max_age_days": 30,
            },
            {
                "name": "other-60-to-90-days",
                "data_stream_patterns": ["logs-hdg-hdg", "logs-standalone-*"],
                "min_age_days": 60,
                "max_age_days": 90,
            },
        ]

    def snapshot(self, name, archived_days_ago, data_streams):
        completed = self.now - dt.timedelta(days=archived_days_ago)
        return {
            "snapshot": name,
            "uuid": f"{name}-uuid",
            "state": "SUCCESS",
            "end_time_in_millis": int(completed.timestamp() * 1000),
            "data_streams": data_streams,
            "metadata": {"managed_by": "elk-cold-archive"},
        }

    def test_expiry_uses_archive_success_time_and_remaining_rule_window(self):
        snapshots = [
            self.snapshot("cold-archive-gateway-expired", 23, ["logs-gateway-prod"]),
            self.snapshot("cold-archive-nginx-expired", 15, ["logs-nginx-prod"]),
            self.snapshot("cold-archive-other-expired", 30, ["logs-hdg-hdg"]),
            self.snapshot("cold-archive-gateway-current", 22, ["logs-gateway-prod"]),
            self.snapshot("cold-archive-nginx-current", 14, ["logs-nginx-prod"]),
            self.snapshot("cold-archive-other-current", 29, ["logs-standalone-prod"]),
            {
                **self.snapshot("cold-archive-manual", 100, ["logs-gateway-prod"]),
                "metadata": {},
            },
            {
                **self.snapshot("cold-archive-failed", 100, ["logs-gateway-prod"]),
                "state": "FAILED",
            },
        ]

        plan = archive.plan_expired_snapshots(
            snapshots,
            self.rules,
            "cold-archive",
            self.now,
            max_deletions=10,
        )

        self.assertEqual(
            [
                "cold-archive-other-expired",
                "cold-archive-gateway-expired",
                "cold-archive-nginx-expired",
            ],
            [item.name for item in plan.expired],
        )
        self.assertEqual([30, 23, 15], [item.retention_days for item in plan.expired])


    def test_mixed_snapshot_uses_the_longest_matching_retention(self):
        snapshots = [
            self.snapshot(
                "cold-archive-mixed",
                29,
                ["logs-gateway-prod", "logs-hdg-hdg"],
            )
        ]

        plan = archive.plan_expired_snapshots(
            snapshots,
            self.rules,
            "cold-archive",
            self.now,
            max_deletions=10,
        )

        self.assertFalse(plan.expired)
        self.assertFalse(plan.warnings)

    def test_recorded_retention_is_not_recomputed_from_changed_rules(self):
        recorded_long = self.snapshot(
            "cold-archive-recorded-long", 24, ["logs-gateway-prod"]
        )
        recorded_long["metadata"]["retention_days"] = 30
        recorded_short = self.snapshot(
            "cold-archive-recorded-short", 15, ["logs-gateway-prod"]
        )
        recorded_short["metadata"]["retention_days"] = 15

        plan = archive.plan_expired_snapshots(
            [recorded_long, recorded_short],
            self.rules,
            "cold-archive",
            self.now,
            max_deletions=10,
        )

        self.assertEqual(
            ["cold-archive-recorded-short"],
            [item.name for item in plan.expired],
        )
        self.assertEqual(15, plan.expired[0].retention_days)

    def test_recorded_retention_survives_rule_removal(self):
        snapshot = self.snapshot(
            "cold-archive-rule-removed", 15, ["logs-retired-prod"]
        )
        snapshot["metadata"]["retention_days"] = 15

        plan = archive.plan_expired_snapshots(
            [snapshot],
            [],
            "cold-archive",
            self.now,
            max_deletions=10,
        )

        self.assertEqual(
            ["cold-archive-rule-removed"],
            [item.name for item in plan.expired],
        )
        self.assertEqual(15, plan.expired[0].retention_days)

    def test_invalid_recorded_retention_is_skipped(self):
        snapshot = self.snapshot(
            "cold-archive-invalid-retention", 100, ["logs-gateway-prod"]
        )
        snapshot["metadata"]["retention_days"] = "23"

        plan = archive.plan_expired_snapshots(
            [snapshot],
            self.rules,
            "cold-archive",
            self.now,
            max_deletions=10,
        )

        self.assertFalse(plan.expired)
        self.assertTrue(
            any("invalid recorded retention" in warning for warning in plan.warnings)
        )

    def test_cleanup_limit_processes_oldest_snapshots_first(self):
        snapshots = [
            self.snapshot("cold-archive-23", 23, ["logs-gateway-prod"]),
            self.snapshot("cold-archive-24", 24, ["logs-gateway-prod"]),
            self.snapshot("cold-archive-25", 25, ["logs-gateway-prod"]),
        ]

        plan = archive.plan_expired_snapshots(
            snapshots,
            self.rules,
            "cold-archive",
            self.now,
            max_deletions=2,
        )

        self.assertEqual(
            ["cold-archive-25", "cold-archive-24"],
            [item.name for item in plan.expired],
        )
        self.assertIn("2 of 3", plan.warnings[0])

class SnapshotCleanupExecutionTests(unittest.TestCase):
    def test_dry_run_reports_expired_without_delete_request(self):
        now = dt.datetime(2026, 8, 10, 0, 0, tzinfo=dt.timezone.utc)
        completed = now - dt.timedelta(days=31)
        snapshot = {
            "snapshot": "cold-archive-expired",
            "uuid": "expired-uuid",
            "state": "SUCCESS",
            "end_time_in_millis": int(completed.timestamp() * 1000),
            "data_streams": ["logs-gateway-prod"],
            "metadata": {"managed_by": "elk-cold-archive"},
        }

        class DryRunClient:
            def __init__(self):
                self.calls = []

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if path.endswith("/_status"):
                    return 200, {"snapshots": []}
                if method == "GET" and path.startswith("/_snapshot/repo/cold-archive-*"):
                    return 200, {"snapshots": [snapshot]}
                raise AssertionError(f"Unexpected request: {method} {path}")

        rules = [
            {
                "name": "gateway-7-to-30-days",
                "data_stream_patterns": ["logs-gateway-*"],
                "min_age_days": 7,
                "max_age_days": 30,
            }
        ]
        client = DryRunClient()
        with tempfile.TemporaryDirectory() as directory:
            audit = archive.AuditLog(pathlib.Path(directory) / "audit.jsonl")
            summary = archive.RunSummary(mode="DRY-RUN", started_at="now")
            archive.cleanup_expired_snapshots(
                client,
                "repo",
                "cold-archive",
                rules,
                {"max_deletions_per_run": 10},
                False,
                audit,
                summary,
                now,
            )

        self.assertEqual(["cold-archive-expired"], summary.expired_snapshots)
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_live_mode_deletes_exact_snapshot_and_confirms_absence(self):
        now = dt.datetime(2026, 8, 10, 0, 0, tzinfo=dt.timezone.utc)
        completed = now - dt.timedelta(days=31)
        snapshot = {
            "snapshot": "cold-archive-expired",
            "uuid": "expired-uuid",
            "state": "SUCCESS",
            "end_time_in_millis": int(completed.timestamp() * 1000),
            "data_streams": ["logs-gateway-prod"],
            "metadata": {"managed_by": "elk-cold-archive"},
        }

        class LiveClient:
            def __init__(self):
                self.calls = []
                self.deleted = False

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if path.endswith("/_status"):
                    return 200, {"snapshots": []}
                if method == "GET" and path.startswith("/_snapshot/repo/cold-archive-*"):
                    return 200, {"snapshots": [snapshot]}
                if method == "GET" and path == "/_snapshot/repo/cold-archive-expired":
                    if self.deleted:
                        return 404, None
                    return 200, {"snapshots": [snapshot]}
                if method == "DELETE" and path == "/_snapshot/repo/cold-archive-expired":
                    self.deleted = True
                    return 200, {"acknowledged": True}
                raise AssertionError(f"Unexpected request: {method} {path}")

        rules = [
            {
                "name": "gateway-7-to-30-days",
                "data_stream_patterns": ["logs-gateway-*"],
                "min_age_days": 7,
                "max_age_days": 30,
            }
        ]
        client = LiveClient()
        with tempfile.TemporaryDirectory() as directory:
            audit = archive.AuditLog(pathlib.Path(directory) / "audit.jsonl")
            summary = archive.RunSummary(mode="LIVE", started_at="now")
            archive.cleanup_expired_snapshots(
                client,
                "repo",
                "cold-archive",
                rules,
                {"max_deletions_per_run": 10},
                True,
                audit,
                summary,
                now,
            )

        self.assertEqual(["cold-archive-expired"], summary.deleted_snapshots)
        delete_paths = [path for method, path in client.calls if method == "DELETE"]
        self.assertEqual(["/_snapshot/repo/cold-archive-expired"], delete_paths)

    def test_live_mode_revalidates_snapshot_immediately_before_delete(self):
        now = dt.datetime(2026, 8, 10, 0, 0, tzinfo=dt.timezone.utc)

        def snapshot(age_days):
            completed = now - dt.timedelta(days=age_days)
            return {
                "snapshot": "cold-archive-reused-name",
                "uuid": f"uuid-{age_days}",
                "state": "SUCCESS",
                "end_time_in_millis": int(completed.timestamp() * 1000),
                "data_streams": ["logs-gateway-prod"],
                "metadata": {"managed_by": "elk-cold-archive"},
            }

        class ReplacedSnapshotClient:
            def __init__(self):
                self.calls = []

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if path.endswith("/_status"):
                    return 200, {"snapshots": []}
                if method == "GET" and path.startswith("/_snapshot/repo/cold-archive-*"):
                    return 200, {"snapshots": [snapshot(31)]}
                if method == "GET" and path == "/_snapshot/repo/cold-archive-reused-name":
                    return 200, {"snapshots": [snapshot(1)]}
                if method == "DELETE":
                    raise AssertionError("replacement snapshot must not be deleted")
                raise AssertionError(f"Unexpected request: {method} {path}")

        rules = [
            {
                "name": "gateway-7-to-30-days",
                "data_stream_patterns": ["logs-gateway-*"],
                "min_age_days": 7,
                "max_age_days": 30,
            }
        ]
        client = ReplacedSnapshotClient()
        with tempfile.TemporaryDirectory() as directory:
            audit = archive.AuditLog(pathlib.Path(directory) / "audit.jsonl")
            summary = archive.RunSummary(mode="LIVE", started_at="now")
            with self.assertRaises(archive.ArchiveError):
                archive.cleanup_expired_snapshots(
                    client,
                    "repo",
                    "cold-archive",
                    rules,
                    {"max_deletions_per_run": 10},
                    True,
                    audit,
                    summary,
                    now,
                )

        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))


class SnapshotBatchingTests(unittest.TestCase):
    def test_candidates_use_fixed_retention_from_configured_window(self):
        def candidate(index_name, stream, rule, age_days):
            return archive.Candidate(
                index=index_name,
                index_uuid=f"{index_name}-uuid",
                data_stream=stream,
                rule=rule,
                rollover_time_ms=1,
                rollover_local="2026-01-01T00:00:00+08:00",
                age_days=age_days,
            )

        candidates = [
            candidate("gateway-age-23", "logs-gateway-prod", "gateway-7-to-30-days", 23),
            candidate("gateway-age-7", "logs-gateway-prod", "gateway-7-to-30-days", 7),
            candidate("nginx-age-20", "logs-nginx-prod", "nginx-15-to-30-days", 20),
            candidate("hdg-age-75", "logs-hdg-hdg", "other-60-to-90-days", 75),
        ]
        rules = [
            {
                "name": "gateway-7-to-30-days",
                "min_age_days": 7,
                "max_age_days": 30,
            },
            {
                "name": "nginx-15-to-30-days",
                "min_age_days": 15,
                "max_age_days": 30,
            },
            {
                "name": "other-60-to-90-days",
                "min_age_days": 60,
                "max_age_days": 90,
            },
        ]

        groups = archive.group_candidates_by_retention(candidates, rules)

        self.assertEqual(
            [
                (23, ["gateway-age-23", "gateway-age-7"]),
                (15, ["nginx-age-20"]),
                (30, ["hdg-age-75"]),
            ],
            [(days, [item.index for item in group]) for days, group in groups],
        )


class RunArchiveCleanupIntegrationTests(unittest.TestCase):
    def test_enabled_cleanup_runs_in_dry_run_without_new_archive_candidates(self):
        completed = archive.utc_now() - dt.timedelta(days=31)
        expired_snapshot = {
            "snapshot": "cold-archive-expired",
            "uuid": "expired-uuid",
            "state": "SUCCESS",
            "end_time_in_millis": int(completed.timestamp() * 1000),
            "data_streams": ["logs-gateway-prod"],
            "metadata": {"managed_by": "elk-cold-archive"},
        }

        class IntegrationClient:
            def __init__(self):
                self.calls = []

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if method == "GET" and path == "/":
                    return 200, {
                        "version": {"number": "8.19.12"},
                        "cluster_uuid": "cluster-uuid",
                    }
                if method == "GET" and path.startswith("/_cluster/health"):
                    return 200, {"status": "yellow", "timed_out": False}
                if method == "GET" and path == "/_snapshot/repo":
                    return 200, {
                        "repo": {"type": "s3", "uuid": "repo-uuid", "settings": {}}
                    }
                if method == "GET" and path.startswith("/_data_stream"):
                    return 200, {
                        "data_streams": [
                            {
                                "name": "logs-gateway-prod",
                                "indices": [index("gateway-write", "write-uuid")],
                            }
                        ]
                    }
                if method == "GET" and path == "/_snapshot/repo/_status":
                    return 200, {"snapshots": []}
                if method == "GET" and path.startswith("/_snapshot/repo/cold-archive-*"):
                    return 200, {"snapshots": [expired_snapshot]}
                raise AssertionError(f"Unexpected request: {method} {path}")

        config = {
            "elasticsearch": {
                "repository": "repo",
                "snapshot_prefix": "cold-archive",
                "expected_version": "8.19.12",
            },
            "repository_guard": {"uuid": "repo-uuid", "type": "s3", "settings": {}},
            "snapshot_cleanup": {"enabled": True, "max_deletions_per_run": 10},
            "selection": {
                "timezone": "Asia/Shanghai",
                "rules": [
                    {
                        "name": "gateway-7-to-30-days",
                        "data_stream_patterns": ["logs-gateway-*"],
                        "min_age_days": 7,
                        "max_age_days": 30,
                    }
                ],
                "max_indices_per_run": 100,
                "max_data_streams_per_snapshot": 10,
            },
            "execution": {
                "snapshot_poll_seconds": 1,
                "snapshot_timeout_seconds": 2,
                "snapshot_missing_grace_seconds": 0,
                "max_run_seconds": 60,
            },
        }
        client = IntegrationClient()
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            audit = archive.AuditLog(base / "audit.jsonl")
            state_store = archive.StateStore(base / "state.json")
            summary = archive.RunSummary(mode="DRY-RUN", started_at="now")
            archive.run_archive(client, config, False, summary, audit, state_store)

        self.assertEqual(["cold-archive-expired"], summary.expired_snapshots)
        self.assertFalse(summary.candidates)
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_live_run_deletes_expired_snapshot_through_full_entrypoint(self):
        completed = archive.utc_now() - dt.timedelta(days=31)
        expired_snapshot = {
            "snapshot": "cold-archive-expired",
            "uuid": "expired-uuid",
            "state": "SUCCESS",
            "end_time_in_millis": int(completed.timestamp() * 1000),
            "data_streams": ["logs-gateway-prod"],
            "metadata": {"managed_by": "elk-cold-archive"},
        }

        class LiveIntegrationClient:
            def __init__(self):
                self.calls = []
                self.deleted = False

            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if method == "GET" and path == "/":
                    return 200, {
                        "version": {"number": "8.19.12"},
                        "cluster_uuid": "cluster-uuid",
                    }
                if method == "GET" and path.startswith("/_cluster/health"):
                    return 200, {"status": "yellow", "timed_out": False}
                if method == "GET" and path == "/_snapshot/repo":
                    return 200, {
                        "repo": {"type": "s3", "uuid": "repo-uuid", "settings": {}}
                    }
                if method == "POST" and path == "/_snapshot/repo/_verify":
                    return 200, {"nodes": {"node-1": {}}}
                if method == "GET" and path.startswith("/_data_stream"):
                    return 200, {
                        "data_streams": [
                            {
                                "name": "logs-gateway-prod",
                                "indices": [index("gateway-write", "write-uuid")],
                            }
                        ]
                    }
                if method == "GET" and path == "/_snapshot/repo/_status":
                    return 200, {"snapshots": []}
                if method == "GET" and path.startswith("/_snapshot/repo/cold-archive-*"):
                    return 200, {"snapshots": [expired_snapshot]}
                if method == "GET" and path == "/_snapshot/repo/cold-archive-expired":
                    if self.deleted:
                        return 404, None
                    return 200, {"snapshots": [expired_snapshot]}
                if method == "DELETE" and path == "/_snapshot/repo/cold-archive-expired":
                    self.deleted = True
                    return 200, {"acknowledged": True}
                raise AssertionError(f"Unexpected request: {method} {path}")

        config = {
            "elasticsearch": {
                "repository": "repo",
                "snapshot_prefix": "cold-archive",
                "expected_version": "8.19.12",
                "expected_cluster_uuid": "cluster-uuid",
            },
            "repository_guard": {"uuid": "repo-uuid", "type": "s3", "settings": {}},
            "snapshot_cleanup": {"enabled": True, "max_deletions_per_run": 10},
            "selection": {
                "timezone": "Asia/Shanghai",
                "rules": [
                    {
                        "name": "gateway-7-to-30-days",
                        "data_stream_patterns": ["logs-gateway-*"],
                        "min_age_days": 7,
                        "max_age_days": 30,
                    }
                ],
                "max_indices_per_run": 100,
                "max_data_streams_per_snapshot": 10,
            },
            "execution": {
                "snapshot_poll_seconds": 1,
                "snapshot_timeout_seconds": 2,
                "snapshot_missing_grace_seconds": 0,
                "max_run_seconds": 60,
            },
        }
        client = LiveIntegrationClient()
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            audit = archive.AuditLog(base / "audit.jsonl")
            state_store = archive.StateStore(base / "state.json")
            summary = archive.RunSummary(mode="LIVE", started_at="now")
            archive.run_archive(client, config, True, summary, audit, state_store)

        self.assertEqual(["cold-archive-expired"], summary.deleted_snapshots)
        self.assertEqual(
            ["/_snapshot/repo/cold-archive-expired"],
            [path for method, path in client.calls if method == "DELETE"],
        )


class FakeTransactionClient:
    def __init__(self, streams, snapshot):
        self.streams = streams
        self.snapshot = snapshot
        self.calls = []
        self.deleted = set()

    def request(self, method, path, body=None, allowed_status=None, timeout=None):
        self.calls.append((method, path))
        if method == "GET" and path.startswith("/_snapshot/"):
            return 200, {"snapshots": [self.snapshot]}
        if method == "GET" and path.startswith("/_data_stream"):
            return 200, {"data_streams": self.streams}
        if method == "DELETE":
            self.deleted.add(path.removeprefix("/"))
            return 200, {"acknowledged": True}
        if method == "HEAD":
            name = path.removeprefix("/")
            return (404, None) if name in self.deleted else (200, None)
        raise AssertionError(f"Unexpected request: {method} {path}")


class TransactionSafetyTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = {
            "state": "SUCCESS",
            "indices": ["old-index", "write-index"],
            "data_streams": ["logs-gateway-prod"],
            "shards": {"total": 1, "successful": 1, "failed": 0},
            "failures": [],
        }
        self.item = {
            "index": "old-index",
            "index_uuid": "old-uuid",
            "data_stream": "logs-gateway-prod",
            "rule": "gateway",
            "rollover_time_ms": 1,
            "rollover_local": "2026-01-01T00:00:00+08:00",
            "age_days": 7,
            "rollover_time_source": "override",
            "rollover_override_reason": "Reviewed restored backing index",
        }
        self.execution = {
            "snapshot_poll_seconds": 0,
            "snapshot_timeout_seconds": 1,
            "snapshot_missing_grace_seconds": 0,
        }

    def state(self):
        return {
            "schema_version": 2,
            "repository": "repo",
            "snapshot": "snapshot-1",
            "cluster_uuid": "cluster-uuid",
            "data_streams": ["logs-gateway-prod"],
            "phase": "waiting",
            "created_at": "2026-07-14T00:00:00+00:00",
            "indices": [dict(self.item)],
            "deleted": [],
        }

    def run_transaction(self, client, state):
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            store = archive.StateStore(base / "state.json")
            audit = archive.AuditLog(base / "audit.jsonl")
            summary = archive.RunSummary(
                mode="LIVE", started_at="now", cluster_uuid="cluster-uuid"
            )
            store.save(state)
            archive.complete_or_resume_transaction(
                client, self.execution, store, audit, state, summary
            )
            return store.load(), summary

    def test_delete_occurs_only_after_snapshot_and_uuid_checks(self):
        streams = [
            {
                "name": "logs-gateway-prod",
                "indices": [
                    index("old-index", "old-uuid"),
                    index("write-index", "write-uuid"),
                ],
            }
        ]
        client = FakeTransactionClient(streams, self.snapshot)
        remaining_state, summary = self.run_transaction(client, self.state())
        self.assertIsNone(remaining_state)
        self.assertEqual(["old-index"], summary.deleted)
        methods = [method for method, _ in client.calls]
        delete_position = methods.index("DELETE")
        self.assertTrue(all(method == "GET" for method in methods[:delete_position]))
        self.assertEqual("HEAD", methods[delete_position + 1])

    def test_current_write_index_is_never_deleted(self):
        streams = [
            {"name": "logs-gateway-prod", "indices": [index("old-index", "old-uuid")]}
        ]
        client = FakeTransactionClient(streams, self.snapshot)
        with self.assertRaises(archive.ArchiveError):
            self.run_transaction(client, self.state())
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_changed_uuid_is_never_deleted(self):
        streams = [
            {
                "name": "logs-gateway-prod",
                "indices": [
                    index("old-index", "recreated-uuid"),
                    index("write-index", "write-uuid"),
                ],
            }
        ]
        client = FakeTransactionClient(streams, self.snapshot)
        with self.assertRaises(archive.ArchiveError):
            self.run_transaction(client, self.state())
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_missing_expected_uuid_is_never_deleted(self):
        streams = [
            {
                "name": "logs-gateway-prod",
                "indices": [index("old-index", "uuid"), index("write-index", "write-uuid")],
            }
        ]
        state = self.state()
        state["indices"][0]["index_uuid"] = ""
        client = FakeTransactionClient(streams, self.snapshot)
        with self.assertRaises(archive.ArchiveError):
            self.run_transaction(client, state)
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_aborted_snapshot_is_archived_and_never_deleted(self):
        aborted = dict(self.snapshot)
        aborted["state"] = "ABORTED"
        client = FakeTransactionClient([], aborted)
        with self.assertRaises(archive.ArchiveError):
            self.run_transaction(client, self.state())
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))

    def test_missing_recorded_snapshot_is_archived_and_never_deleted(self):
        class MissingSnapshotClient(FakeTransactionClient):
            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.calls.append((method, path))
                if method == "GET" and path.startswith("/_snapshot/"):
                    return 404, None
                raise AssertionError(f"Unexpected request: {method} {path}")

        client = MissingSnapshotClient([], self.snapshot)
        with self.assertRaises(archive.SnapshotMissingError):
            self.run_transaction(client, self.state())
        self.assertFalse(any(method == "DELETE" for method, _ in client.calls))


class PreflightSafetyTests(unittest.TestCase):
    def test_live_mode_requires_expected_cluster_uuid(self):
        class RootOnlyClient:
            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                self.path = path
                return 200, {"version": {"number": "8.19.2"}, "cluster_uuid": "actual-uuid"}

        config = {
            "elasticsearch": {
                "expected_version": "8.19.2",
                "expected_cluster_uuid": "",
                "repository": "repo",
            },
            "repository_guard": {},
        }
        summary = archive.RunSummary(mode="LIVE", started_at="now")
        with self.assertRaises(archive.ConfigError):
            archive.preflight(RootOnlyClient(), config, True, summary)
        self.assertEqual("actual-uuid", summary.cluster_uuid)

    def test_live_mode_requires_nonempty_repository_uuid_guard(self):
        class PreflightClient:
            def request(self, method, path, body=None, allowed_status=None, timeout=None):
                if method == "GET" and path == "/":
                    return 200, {
                        "version": {"number": "8.19.12"},
                        "cluster_uuid": "cluster-uuid",
                    }
                if method == "GET" and path.startswith("/_cluster/health"):
                    return 200, {"status": "yellow", "timed_out": False}
                if method == "GET" and path == "/_snapshot/repo":
                    return 200, {
                        "repo": {"type": "s3", "uuid": "actual-uuid", "settings": {}}
                    }
                raise AssertionError(f"Unexpected request: {method} {path}")

        config = {
            "elasticsearch": {
                "expected_version": "8.19.12",
                "expected_cluster_uuid": "cluster-uuid",
                "repository": "repo",
            },
            "repository_guard": {},
        }
        invalid_guards = [
            {"type": "s3", "settings": {}},
            {"uuid": "", "type": "s3", "settings": {}},
        ]
        for guard in invalid_guards:
            with self.subTest(guard=guard):
                config["repository_guard"] = guard
                summary = archive.RunSummary(mode="LIVE", started_at="now")
                with self.assertRaisesRegex(archive.ConfigError, "repository_guard.uuid"):
                    archive.preflight(PreflightClient(), config, True, summary)


class ConfigAndStateTests(unittest.TestCase):
    @staticmethod
    def valid_config():
        return {
            "elasticsearch": {"url": "https://example.test", "repository": "repo"},
            "repository_guard": {},
            "selection": {
                "timezone": "Asia/Shanghai",
                "rules": [
                    {
                        "name": "gateway",
                        "data_stream_patterns": ["logs-gateway-*"],
                        "min_age_days": 7,
                        "max_age_days": 30,
                    }
                ],
                "max_indices_per_run": 100,
                "max_data_streams_per_snapshot": 10,
            },
            "execution": {
                "lock_file": "l",
                "state_file": "s",
                "audit_log": "a",
                "snapshot_poll_seconds": 1,
                "snapshot_timeout_seconds": 2,
                "snapshot_missing_grace_seconds": 0,
                "max_run_seconds": 3,
            },
        }

    def test_rollover_override_requires_review_reason(self):
        config = self.valid_config()
        config["selection"]["rollover_time_overrides"] = [
            {
                "index": ".ds-logs-gateway-prod-2026.08.04-000001",
                "index_uuid": "index-uuid",
                "rollover_time_ms": 1786605291333,
            }
        ]

        with self.assertRaisesRegex(archive.ConfigError, "reason"):
            archive.validate_config(config)

    def test_rollover_override_reason_rejects_control_characters(self):
        config = self.valid_config()
        config["selection"]["rollover_time_overrides"] = [
            {
                "index": ".ds-logs-gateway-prod-2026.08.04-000001",
                "index_uuid": "index-uuid",
                "rollover_time_ms": 1786605291333,
                "reason": "Reviewed\nforged log line",
            }
        ]

        with self.assertRaisesRegex(archive.ConfigError, "control characters"):
            archive.validate_config(config)

    def test_rollover_override_rejects_unknown_fields(self):
        config = self.valid_config()
        config["selection"]["rollover_time_overrides"] = [
            {
                "index": ".ds-logs-gateway-prod-2026.08.04-000001",
                "index_uuid": "index-uuid",
                "rollover_time_ms": 1786605291333,
                "reason": "Reviewed restored backing index",
                "rollover_time": "2026-08-13T07:14:51.333Z",
            }
        ]

        with self.assertRaisesRegex(archive.ConfigError, "unknown fields"):
            archive.validate_config(config)

    def test_rollover_override_rejects_unsafe_identity_and_time(self):
        valid = {
            "index": ".ds-logs-gateway-prod-2026.08.04-000001",
            "index_uuid": "index-uuid",
            "rollover_time_ms": 1786605291333,
            "reason": "Reviewed restored backing index",
        }
        invalid_overrides = [
            {key: value for key, value in valid.items() if key != "index"},
            {**valid, "index": ""},
            {**valid, "index": "logs-*"},
            {**valid, "index": "one,two"},
            {**valid, "index": "one/two"},
            {**valid, "index": "index\x00name"},
            {**valid, "index": "index\x7fname"},
            {key: value for key, value in valid.items() if key != "index_uuid"},
            {**valid, "index_uuid": ""},
            {**valid, "index_uuid": "uuid\x00value"},
            {**valid, "index_uuid": "uuid\x7fvalue"},
            {key: value for key, value in valid.items() if key != "rollover_time_ms"},
            {**valid, "rollover_time_ms": True},
            {**valid, "rollover_time_ms": 1.5},
            {**valid, "rollover_time_ms": "1786605291333"},
            {**valid, "rollover_time_ms": 0},
            {**valid, "rollover_time_ms": -1},
            {**valid, "rollover_time_ms": 10**30},
        ]

        for override in invalid_overrides:
            with self.subTest(override=override):
                config = self.valid_config()
                config["selection"]["rollover_time_overrides"] = [override]
                with self.assertRaises(archive.ConfigError):
                    archive.validate_config(config)

    def test_rollover_override_rejects_duplicate_index(self):
        override = {
            "index": ".ds-logs-gateway-prod-2026.08.04-000001",
            "index_uuid": "index-uuid",
            "rollover_time_ms": 1786605291333,
            "reason": "Reviewed restored backing index",
        }
        config = self.valid_config()
        config["selection"]["rollover_time_overrides"] = [
            dict(override),
            dict(override),
        ]

        with self.assertRaisesRegex(archive.ConfigError, "Duplicate rollover override"):
            archive.validate_config(config)

    def test_valid_rollover_override_config_is_accepted(self):
        config = self.valid_config()
        config["selection"]["rollover_time_overrides"] = [
            {
                "index": ".ds-logs-gateway-prod-2026.08.04-000001",
                "index_uuid": "index-uuid",
                "rollover_time_ms": 1786605291333,
                "reason": "Reviewed restored backing index",
            }
        ]

        archive.validate_config(config)

    def test_slash_in_pattern_is_rejected(self):
        config = {
            "elasticsearch": {"url": "https://example.test", "repository": "repo"},
            "repository_guard": {},
            "selection": {
                "timezone": "Asia/Shanghai",
                "rules": [
                    {
                        "name": "bad",
                        "data_stream_patterns": ["one/two"],
                        "min_age_days": 1,
                        "max_age_days": 2,
                    }
                ],
                "max_indices_per_run": 2,
                "max_data_streams_per_snapshot": 1,
            },
            "execution": {
                "lock_file": "l",
                "state_file": "s",
                "audit_log": "a",
                "snapshot_poll_seconds": 1,
                "snapshot_timeout_seconds": 2,
                "snapshot_missing_grace_seconds": 0,
                "max_run_seconds": 3
            },
        }
        with self.assertRaises(archive.ConfigError):
            archive.validate_config(config)

    def test_state_write_is_atomic_and_round_trips(self):
        with tempfile.TemporaryDirectory() as directory:
            store = archive.StateStore(pathlib.Path(directory) / "state.json")
            state = {"schema_version": 2, "phase": "waiting"}
            store.save(state)
            self.assertEqual(state, store.load())
            store.clear()
            self.assertIsNone(store.load())


if __name__ == "__main__":
    unittest.main()
