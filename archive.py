#!/usr/bin/env python3
"""Archive old Elasticsearch data-stream backing indices to a snapshot repository."""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import datetime as dt
import email.message
import fnmatch
import json
import logging
import os
import pathlib
import re
import smtplib
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Iterable
from zoneinfo import ZoneInfo

try:
    import fcntl
except ImportError:  # Allows unit tests to import the Linux-targeted module on Windows.
    fcntl = None  # type: ignore[assignment]


VERSION = "1.2.0"
LOG = logging.getLogger("elk-cold-archive")
TERMINAL_SNAPSHOT_STATES = {"SUCCESS", "FAILED", "PARTIAL", "ABORTED", "INCOMPATIBLE"}
RETRYABLE_HTTP_STATUS = {429, 502, 503, 504}


class ArchiveError(RuntimeError):
    """Expected operational or safety failure."""


class ConfigError(ArchiveError):
    """Invalid local configuration."""


class ApiError(ArchiveError):
    def __init__(self, method: str, path: str, status: int, response: str):
        self.method = method
        self.path = path
        self.status = status
        self.response = response
        super().__init__(f"Elasticsearch {method} {path} returned HTTP {status}: {response[:500]}")


class SnapshotMissingError(ArchiveError):
    """A locally recorded snapshot transaction has no matching repository snapshot."""


@dataclasses.dataclass(frozen=True)
class Candidate:
    index: str
    index_uuid: str
    data_stream: str
    rule: str
    rollover_time_ms: int
    rollover_local: str
    age_days: int
    rollover_time_source: str = "cluster_state"
    rollover_override_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class RolloverTimeOverride:
    index: str
    index_uuid: str
    rollover_time_ms: int
    reason: str


@dataclasses.dataclass
class Selection:
    candidates: list[Candidate]
    overdue: list[Candidate]
    warnings: list[str]


@dataclasses.dataclass(frozen=True)
class SnapshotExpiry:
    name: str
    uuid: str
    archived_at: str
    retention_days: int
    data_streams: tuple[str, ...]
    rules: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        value = dataclasses.asdict(self)
        value["data_streams"] = list(self.data_streams)
        value["rules"] = list(self.rules)
        return value


@dataclasses.dataclass
class SnapshotCleanupPlan:
    expired: list[SnapshotExpiry]
    warnings: list[str]


@dataclasses.dataclass
class RunSummary:
    mode: str
    started_at: str
    finished_at: str = ""
    cluster_version: str = ""
    cluster_uuid: str = ""
    candidates: list[str] = dataclasses.field(default_factory=list)
    overdue: list[str] = dataclasses.field(default_factory=list)
    snapshots: list[str] = dataclasses.field(default_factory=list)
    deleted: list[str] = dataclasses.field(default_factory=list)
    expired_snapshots: list[str] = dataclasses.field(default_factory=list)
    deleted_snapshots: list[str] = dataclasses.field(default_factory=list)
    warnings: list[str] = dataclasses.field(default_factory=list)
    errors: list[str] = dataclasses.field(default_factory=list)

    def render(self) -> str:
        lines = [
            f"Mode: {self.mode}",
            f"Started: {self.started_at}",
            f"Finished: {self.finished_at or '-'}",
            f"Elasticsearch: {self.cluster_version or '-'}",
            f"Cluster UUID: {self.cluster_uuid or '-'}",
            f"Candidates ({len(self.candidates)}): {', '.join(self.candidates) or '-'}",
            f"Overdue, not deleted ({len(self.overdue)}): {', '.join(self.overdue) or '-'}",
            f"Snapshots ({len(self.snapshots)}): {', '.join(self.snapshots) or '-'}",
            f"Deleted ({len(self.deleted)}): {', '.join(self.deleted) or '-'}",
            f"Expired snapshots ({len(self.expired_snapshots)}): "
            f"{', '.join(self.expired_snapshots) or '-'}",
            f"Deleted snapshots ({len(self.deleted_snapshots)}): "
            f"{', '.join(self.deleted_snapshots) or '-'}",
        ]
        if self.warnings:
            lines.append("Warnings:\n- " + "\n- ".join(self.warnings))
        if self.errors:
            lines.append("Errors:\n- " + "\n- ".join(self.errors))
        return "\n".join(lines)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def oss_retention_days(rule: dict[str, Any]) -> int:
    return int(rule["max_age_days"]) - int(rule["min_age_days"])


def group_candidates_by_retention(
    candidates: list[Candidate], rules: list[dict[str, Any]]
) -> list[tuple[int, list[Candidate]]]:
    rules_by_name = {str(rule["name"]): rule for rule in rules}
    groups: dict[int, list[Candidate]] = {}
    for candidate in candidates:
        rule = rules_by_name.get(candidate.rule)
        if rule is None:
            raise ConfigError(f"Candidate references unknown rule {candidate.rule!r}")
        retention_days = oss_retention_days(rule)
        groups.setdefault(retention_days, []).append(candidate)
    return list(groups.items())


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"Invalid boolean value: {value!r}")


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Cannot read JSON config {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigError("The configuration root must be a JSON object")
    return value


def require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"Missing {where}.{key}")
    return mapping[key]


def parse_rollover_time_overrides(
    selection: dict[str, Any],
) -> dict[str, RolloverTimeOverride]:
    raw_overrides = selection.get("rollover_time_overrides", [])
    if not isinstance(raw_overrides, list):
        raise ConfigError("selection.rollover_time_overrides must be an array")

    overrides: dict[str, RolloverTimeOverride] = {}
    allowed_fields = {"index", "index_uuid", "rollover_time_ms", "reason"}
    for position, raw in enumerate(raw_overrides):
        where = f"selection.rollover_time_overrides[{position}]"
        if not isinstance(raw, dict):
            raise ConfigError(f"{where} must be an object")
        unknown_fields = sorted(set(raw) - allowed_fields)
        if unknown_fields:
            raise ConfigError(f"{where} contains unknown fields: {', '.join(unknown_fields)}")

        index_name = require(raw, "index", where)
        if not isinstance(index_name, str) or not index_name or index_name != index_name.strip():
            raise ConfigError(f"{where}.index must be an exact non-empty index name")
        if not index_name.isprintable():
            raise ConfigError(f"{where}.index must not contain control characters")
        if any(character in index_name for character in "*?[],/\\"):
            raise ConfigError(f"{where}.index must not contain wildcard, comma, or path characters")
        if index_name in overrides:
            raise ConfigError(f"Duplicate rollover override for index {index_name}")

        index_uuid = require(raw, "index_uuid", where)
        if (
            not isinstance(index_uuid, str)
            or not index_uuid
            or index_uuid != index_uuid.strip()
            or any(character.isspace() for character in index_uuid)
            or not index_uuid.isprintable()
        ):
            raise ConfigError(f"{where}.index_uuid must be an exact non-empty UUID")

        rollover_time_ms = require(raw, "rollover_time_ms", where)
        if type(rollover_time_ms) is not int or rollover_time_ms <= 0:
            raise ConfigError(f"{where}.rollover_time_ms must be a positive integer")
        try:
            dt.datetime.fromtimestamp(rollover_time_ms / 1000, tz=dt.timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ConfigError(f"{where}.rollover_time_ms is outside the supported date range") from exc

        reason = require(raw, "reason", where)
        if not isinstance(reason, str) or not reason.strip():
            raise ConfigError(f"{where}.reason must be a non-empty string")
        if not reason.isprintable():
            raise ConfigError(f"{where}.reason must not contain control characters")
        overrides[index_name] = RolloverTimeOverride(
            index=index_name,
            index_uuid=index_uuid,
            rollover_time_ms=rollover_time_ms,
            reason=reason.strip(),
        )
    return overrides


def validate_config(config: dict[str, Any]) -> None:
    elasticsearch = require(config, "elasticsearch", "config")
    repository_guard = require(config, "repository_guard", "config")
    selection = require(config, "selection", "config")
    execution = require(config, "execution", "config")

    if not isinstance(elasticsearch, dict) or not isinstance(repository_guard, dict):
        raise ConfigError("elasticsearch and repository_guard must be objects")
    url = str(require(elasticsearch, "url", "elasticsearch")).rstrip("/")
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("elasticsearch.url must be an absolute http(s) URL")
    if parsed.username or parsed.password:
        raise ConfigError("Do not embed credentials in elasticsearch.url; use credential variables")
    require(elasticsearch, "repository", "elasticsearch")
    snapshot_prefix = str(elasticsearch.get("snapshot_prefix", "cold-archive"))
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", snapshot_prefix):
        raise ConfigError("elasticsearch.snapshot_prefix must be a lowercase safe name prefix")

    ZoneInfo(str(require(selection, "timezone", "selection")))
    rules = require(selection, "rules", "selection")
    if not isinstance(rules, list) or not rules:
        raise ConfigError("selection.rules must be a non-empty array")
    names: set[str] = set()
    for position, rule in enumerate(rules):
        where = f"selection.rules[{position}]"
        if not isinstance(rule, dict):
            raise ConfigError(f"{where} must be an object")
        name = str(require(rule, "name", where))
        if name in names:
            raise ConfigError(f"Duplicate rule name: {name}")
        names.add(name)
        patterns = require(rule, "data_stream_patterns", where)
        if not isinstance(patterns, list) or not patterns:
            raise ConfigError(f"{where}.data_stream_patterns must be a non-empty array")
        for pattern in patterns:
            if not isinstance(pattern, str) or not pattern:
                raise ConfigError(f"{where} contains an empty pattern")
            if "/" in pattern or "," in pattern:
                raise ConfigError(f"Unsafe data-stream pattern {pattern!r}; split patterns into separate values")
        minimum = int(require(rule, "min_age_days", where))
        maximum = int(require(rule, "max_age_days", where))
        if minimum < 0 or maximum < minimum:
            raise ConfigError(f"Invalid age interval in {where}: {minimum}..{maximum}")

    for key in ("max_indices_per_run", "max_data_streams_per_snapshot"):
        if int(require(selection, key, "selection")) < 1:
            raise ConfigError(f"selection.{key} must be positive")

    parse_rollover_time_overrides(selection)

    for key in ("lock_file", "state_file", "audit_log"):
        if not str(require(execution, key, "execution")):
            raise ConfigError(f"execution.{key} cannot be empty")
    for key in (
        "snapshot_poll_seconds",
        "snapshot_timeout_seconds",
        "snapshot_missing_grace_seconds",
        "max_run_seconds",
    ):
        if int(require(execution, key, "execution")) < 0:
            raise ConfigError(f"execution.{key} cannot be negative")
    if int(execution["snapshot_poll_seconds"]) < 1:
        raise ConfigError("execution.snapshot_poll_seconds must be at least 1")
    if int(execution["snapshot_timeout_seconds"]) < 1 or int(execution["max_run_seconds"]) < 1:
        raise ConfigError("snapshot_timeout_seconds and max_run_seconds must be positive")

    cleanup = config.get("snapshot_cleanup", {})
    if not isinstance(cleanup, dict):
        raise ConfigError("snapshot_cleanup must be an object")
    if not isinstance(cleanup.get("enabled", False), bool):
        raise ConfigError("snapshot_cleanup.enabled must be a boolean")
    if int(cleanup.get("max_deletions_per_run", 10)) < 1:
        raise ConfigError("snapshot_cleanup.max_deletions_per_run must be positive")

def read_secret(value_env: str, file_env: str, credential_name: str) -> str:
    file_name = os.environ.get(file_env, "").strip()
    if not file_name:
        credentials_dir = os.environ.get("CREDENTIALS_DIRECTORY", "").strip()
        if credentials_dir:
            candidate = pathlib.Path(credentials_dir) / credential_name
            if candidate.is_file():
                file_name = str(candidate)
    if file_name:
        try:
            return pathlib.Path(file_name).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigError(f"Cannot read credential file from {file_env}: {exc}") from exc
    return os.environ.get(value_env, "").strip()


def build_auth_header(auth_config: dict[str, Any]) -> str | None:
    auth_type = str(auth_config.get("type", "auto")).lower()
    api_key = read_secret(
        str(auth_config.get("api_key_env", "ES_API_KEY")),
        str(auth_config.get("api_key_file_env", "ES_API_KEY_FILE")),
        "es_api_key",
    )
    username = os.environ.get(str(auth_config.get("username_env", "ES_USERNAME")), "").strip()
    password = read_secret(
        str(auth_config.get("password_env", "ES_PASSWORD")),
        str(auth_config.get("password_file_env", "ES_PASSWORD_FILE")),
        "es_password",
    )
    if auth_type in {"auto", "api_key"} and api_key:
        return f"ApiKey {api_key}"
    if auth_type in {"auto", "basic"} and username and password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"
    if auth_type == "none" or auth_type == "auto":
        return None
    if auth_type == "api_key":
        raise ConfigError("API key auth selected, but ES_API_KEY or ES_API_KEY_FILE is empty")
    if auth_type == "basic":
        raise ConfigError("Basic auth selected, but ES_USERNAME and ES_PASSWORD/ES_PASSWORD_FILE are incomplete")
    raise ConfigError(f"Unsupported authentication type: {auth_type}")


class ElasticsearchClient:
    def __init__(self, config: dict[str, Any]):
        self.base_url = str(config["url"]).rstrip("/")
        self.timeout = float(config.get("request_timeout_seconds", 60))
        self.headers = {"Accept": "application/json", "User-Agent": f"elk-cold-archive/{VERSION}"}
        auth_header = build_auth_header(config.get("auth", {}))
        if auth_header:
            self.headers["Authorization"] = auth_header

        tls_config = config.get("tls", {})
        verify = bool(tls_config.get("verify", True))
        ca_file = os.environ.get(str(tls_config.get("ca_file_env", "ES_CA_CERT")), "").strip()
        if verify:
            self.ssl_context = ssl.create_default_context(cafile=ca_file or None)
        else:
            LOG.warning("TLS certificate verification is disabled")
            self.ssl_context = ssl._create_unverified_context()  # noqa: SLF001
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=self.ssl_context), NoRedirectHandler()
        )

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        allowed_status: set[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, Any]:
        if not path.startswith("/"):
            raise ValueError("Elasticsearch request paths must start with /")
        data = None
        headers = dict(self.headers)
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=data, headers=headers, method=method
        )
        max_attempts = 4 if method in {"GET", "HEAD"} else 1
        for attempt in range(max_attempts):
            try:
                with self.opener.open(request, timeout=timeout or self.timeout) as response:
                    raw = response.read()
                    payload = json.loads(raw) if raw else None
                    return response.status, payload
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                if allowed_status and exc.code in allowed_status:
                    try:
                        payload = json.loads(raw) if raw else None
                    except json.JSONDecodeError:
                        payload = raw
                    return exc.code, payload
                if exc.code in RETRYABLE_HTTP_STATUS and attempt + 1 < max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise ApiError(method, path, exc.code, raw) from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt + 1 < max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise ArchiveError(f"Elasticsearch {method} {path} failed: {exc}") from exc
        raise AssertionError("unreachable")


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Prevent credentials from following an unexpected cross-host redirect."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


class AuditLog:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def write(self, event: str, **details: Any) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {"at": iso_now(), "event": event, **details}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


class StateStore:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ArchiveError(f"Cannot read transaction state {self.path}: {exc}") from exc
        if state.get("schema_version") != 2:
            raise ArchiveError(f"Unsupported transaction state in {self.path}")
        return state

    def save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_name, 0o600)
            os.replace(temp_name, self.path)
            sync_directory(self.path.parent)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temp_name)

    def clear(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()
            sync_directory(self.path.parent)

    def archive_failed(self, state: dict[str, Any]) -> pathlib.Path:
        state["phase"] = "failed"
        state["failed_at"] = iso_now()
        self.save(state)
        suffix = utc_now().strftime("%Y%m%dt%H%M%Sz")
        failed_path = self.path.with_name(f"{self.path.name}.failed.{suffix}")
        os.replace(self.path, failed_path)
        sync_directory(self.path.parent)
        return failed_path


def sync_directory(path: pathlib.Path) -> None:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    if directory_flag == 0:
        return
    descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def exclusive_lock(path: pathlib.Path) -> Iterable[None]:
    if fcntl is None:
        raise ArchiveError("This script requires Linux fcntl file locking")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ArchiveError(f"Another archive process holds {path}") from exc
        yield


def query_path(path: str, **params: Any) -> str:
    values = {key: str(value).lower() if isinstance(value, bool) else str(value) for key, value in params.items()}
    return f"{path}?{urllib.parse.urlencode(values)}" if values else path


def quote_segment(value: str) -> str:
    return urllib.parse.quote(value, safe="._-*")


def normalize_setting(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value).rstrip("/")


def preflight(
    client: ElasticsearchClient,
    config: dict[str, Any],
    execute: bool,
    summary: RunSummary,
) -> None:
    _, root = client.request("GET", "/")
    version = str(root.get("version", {}).get("number", ""))
    cluster_uuid = str(root.get("cluster_uuid", ""))
    summary.cluster_version = version
    summary.cluster_uuid = cluster_uuid
    expected_version = str(config["elasticsearch"].get("expected_version", ""))
    if expected_version and version != expected_version:
        raise ArchiveError(f"Expected Elasticsearch {expected_version}, got {version or 'unknown'}")
    expected_cluster_uuid = os.environ.get("ES_EXPECTED_CLUSTER_UUID", "").strip()
    if not expected_cluster_uuid:
        expected_cluster_uuid = str(
            config["elasticsearch"].get("expected_cluster_uuid", "")
        ).strip()
    if expected_cluster_uuid and cluster_uuid != expected_cluster_uuid:
        raise ArchiveError(
            f"Elasticsearch cluster UUID mismatch: expected {expected_cluster_uuid}, got {cluster_uuid or 'unknown'}"
        )
    if execute and not expected_cluster_uuid:
        raise ConfigError(
            "ES_EXPECTED_CLUSTER_UUID or elasticsearch.expected_cluster_uuid must be set "
            "from a reviewed dry-run before live mode"
        )

    _, health = client.request(
        "GET", query_path("/_cluster/health", wait_for_status="yellow", timeout="30s")
    )
    if health.get("timed_out") or health.get("status") == "red":
        raise ArchiveError(f"Cluster is not safe for archiving: status={health.get('status')}")

    repository = str(config["elasticsearch"]["repository"])
    _, repositories = client.request("GET", f"/_snapshot/{quote_segment(repository)}")
    actual = repositories.get(repository)
    if not isinstance(actual, dict):
        raise ArchiveError(f"Snapshot repository {repository!r} was not returned by Elasticsearch")
    guard = config["repository_guard"]
    expected_repository_uuid = str(guard.get("uuid", "")).strip()
    if not expected_repository_uuid:
        if execute:
            raise ConfigError(
                "repository_guard.uuid must be set from a reviewed snapshot repository response "
                "before live mode"
            )
        summary.warnings.append(
            "repository_guard.uuid is empty; live mode will remain blocked until it is set"
        )
    if normalize_setting(actual.get("type", "")) != normalize_setting(guard.get("type", "")):
        raise ArchiveError("Snapshot repository type does not match repository_guard")
    if expected_repository_uuid and actual.get("uuid") != expected_repository_uuid:
        raise ArchiveError(
            f"Snapshot repository UUID mismatch: expected {expected_repository_uuid}, "
            f"got {actual.get('uuid')}"
        )
    actual_settings = actual.get("settings", {})
    for key, expected in guard.get("settings", {}).items():
        got = actual_settings.get(key)
        if normalize_setting(got) != normalize_setting(expected):
            raise ArchiveError(
                f"Snapshot repository setting {key!r} mismatch: expected {expected!r}, got {got!r}"
            )

    if execute:
        _, verification = client.request("POST", f"/_snapshot/{quote_segment(repository)}/_verify")
        if not isinstance(verification.get("nodes"), dict) or not verification["nodes"]:
            raise ArchiveError(f"Snapshot repository {repository!r} verification returned no nodes")
    else:
        LOG.info("Repository write verification is intentionally skipped in dry-run mode")


def get_data_streams(client: ElasticsearchClient) -> list[dict[str, Any]]:
    _, response = client.request("GET", query_path("/_data_stream", expand_wildcards="all"))
    streams = response.get("data_streams", [])
    if not isinstance(streams, list):
        raise ArchiveError("Unexpected _data_stream response")
    return streams


def build_membership(
    data_streams: list[dict[str, Any]],
) -> tuple[dict[str, tuple[str, str, bool]], list[str]]:
    membership: dict[str, tuple[str, str, bool]] = {}
    errors: list[str] = []
    for stream in data_streams:
        name = str(stream.get("name", ""))
        indices = stream.get("indices", [])
        if not name or not isinstance(indices, list) or not indices:
            errors.append(f"Data stream {name or '<unnamed>'} has no backing indices")
            continue
        write_name = str(indices[-1].get("index_name", ""))
        for entry in indices:
            index_name = str(entry.get("index_name", ""))
            index_uuid = str(entry.get("index_uuid", ""))
            if not index_name:
                errors.append(f"Data stream {name} returned an unnamed backing index")
                continue
            if index_name in membership:
                errors.append(f"Backing index {index_name} appears in multiple data streams")
                continue
            if not index_uuid:
                errors.append(f"Backing index {index_name} has no index_uuid")
                continue
            membership[index_name] = (name, index_uuid, index_name == write_name)
    return membership, errors


def match_stream_rules(
    data_streams: list[dict[str, Any]], rules: list[dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    matches: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    pattern_hits: dict[tuple[str, str], int] = {}
    for rule in rules:
        for pattern in rule["data_stream_patterns"]:
            pattern_hits[(rule["name"], pattern)] = 0

    for stream in data_streams:
        stream_name = str(stream.get("name", ""))
        matching_rules: list[dict[str, Any]] = []
        for rule in rules:
            matched = False
            for pattern in rule["data_stream_patterns"]:
                if fnmatch.fnmatchcase(stream_name, pattern):
                    pattern_hits[(rule["name"], pattern)] += 1
                    matched = True
            if matched:
                matching_rules.append(rule)
        if len(matching_rules) > 1:
            names = ", ".join(str(rule["name"]) for rule in matching_rules)
            errors.append(f"Data stream {stream_name} matches overlapping rules: {names}")
        elif matching_rules:
            matches[stream_name] = matching_rules[0]

    for (rule_name, pattern), count in pattern_hits.items():
        if count == 0:
            errors.append(f"Pattern {pattern!r} in rule {rule_name!r} matched no data streams")
    return matches, errors


def get_rollover_times(
    client: ElasticsearchClient,
    indices: list[tuple[str, str]],
    chunk_size: int = 30,
) -> tuple[dict[str, int], list[str]]:
    """Return actual rollover times from cluster-state metadata.

    indices contains (backing_index_name, owning_data_stream_name) pairs.
    """
    targets = [(index_name, owner, "") for index_name, owner in indices]
    rollover_times, _, _, errors = resolve_rollover_times(
        client, targets, {}, chunk_size=chunk_size
    )
    return rollover_times, errors


def resolve_rollover_times(
    client: ElasticsearchClient,
    indices: list[tuple[str, str, str]],
    overrides: dict[str, RolloverTimeOverride],
    chunk_size: int = 30,
) -> tuple[
    dict[str, int],
    dict[str, RolloverTimeOverride],
    list[str],
    list[str],
]:
    """Resolve authoritative or explicitly reviewed rollover times for non-write indices."""

    target_by_index: dict[str, tuple[str, str]] = {}
    errors: list[str] = []
    for index_name, owner, index_uuid in indices:
        if index_name in target_by_index:
            errors.append(f"Backing index {index_name} appears more than once in rollover targets")
            continue
        target_by_index[index_name] = (owner, index_uuid)

    names = list(target_by_index)
    rollover_times: dict[str, int] = {}
    used_overrides: dict[str, RolloverTimeOverride] = {}
    accounted_overrides: set[str] = set()
    warnings: list[str] = []
    for names_chunk in chunked(names, chunk_size):
        target = ",".join(quote_segment(name) for name in names_chunk)
        path = query_path(
            f"/_cluster/state/metadata/{target}",
            filter_path="metadata.indices.*.rollover_info",
        )
        _, response = client.request("GET", path)
        response_metadata = response.get("metadata") if isinstance(response, dict) else None
        metadata = (
            response_metadata.get("indices")
            if isinstance(response_metadata, dict)
            else None
        )
        if not isinstance(metadata, dict):
            for index_name in names_chunk:
                if index_name in overrides:
                    accounted_overrides.add(index_name)
                errors.append(
                    f"Cluster state returned invalid metadata for backing index {index_name}"
                )
            continue
        for index_name in names_chunk:
            owner, index_uuid = target_by_index[index_name]
            override = overrides.get(index_name)
            if override is not None:
                accounted_overrides.add(index_name)
            if index_name not in metadata:
                errors.append(f"Cluster state returned no metadata for backing index {index_name}")
                continue
            entry = metadata[index_name]
            if not isinstance(entry, dict):
                errors.append(f"Cluster state returned invalid metadata for backing index {index_name}")
                continue
            if "rollover_info" not in entry:
                errors.append(f"Backing index {index_name} has no rollover_info metadata")
                continue
            rollover_info = entry["rollover_info"]
            if not isinstance(rollover_info, dict):
                errors.append(f"Backing index {index_name} has invalid rollover_info metadata")
                continue
            if owner in rollover_info:
                info = rollover_info[owner]
                if not isinstance(info, dict):
                    errors.append(
                        f"Backing index {index_name} has an invalid rollover_info entry "
                        f"for data stream {owner}"
                    )
                    continue
                rollover_time = info.get("time")
                if type(rollover_time) is not int or rollover_time <= 0:
                    errors.append(f"Backing index {index_name} has an invalid rollover time")
                    continue
                rollover_times[index_name] = rollover_time
                if override is not None:
                    errors.append(
                        f"Backing index {index_name} already has authoritative rollover_info; "
                        "remove its rollover override"
                    )
                continue

            if rollover_info:
                errors.append(
                    f"Backing index {index_name} has rollover_info for a different owner; "
                    f"an override for data stream {owner} is not allowed"
                )
                continue

            if override is None:
                errors.append(
                    f"Backing index {index_name} has no rollover_info entry for data stream {owner}"
                )
                continue
            if override.index_uuid != index_uuid:
                errors.append(
                    f"Rollover override UUID mismatch for {index_name}: "
                    f"expected {override.index_uuid}, got {index_uuid or 'missing'}"
                )
                continue

            rollover_times[index_name] = override.rollover_time_ms
            used_overrides[index_name] = override
            reviewed_time = dt.datetime.fromtimestamp(
                override.rollover_time_ms / 1000, tz=dt.timezone.utc
            ).isoformat()
            warnings.append(
                f"Reviewed rollover override used for {index_name} "
                f"uuid={override.index_uuid} time={reviewed_time} reason={override.reason}"
            )

    for index_name in sorted(set(overrides) - accounted_overrides):
        errors.append(
            f"Configured rollover override for {index_name} did not match a current "
            "non-write backing index in the configured data streams"
        )
    return rollover_times, used_overrides, warnings, errors


def select_candidates(
    data_streams: list[dict[str, Any]],
    rollover_times: dict[str, int],
    selection_config: dict[str, Any],
    now: dt.datetime,
    used_overrides: dict[str, RolloverTimeOverride] | None = None,
) -> Selection:
    timezone = ZoneInfo(str(selection_config["timezone"]))
    local_today = now.astimezone(timezone).date()
    resolved_overrides = used_overrides or {}
    rules, match_errors = match_stream_rules(data_streams, selection_config["rules"])
    if match_errors:
        raise ArchiveError("; ".join(match_errors))

    candidates: list[Candidate] = []
    overdue: list[Candidate] = []
    warnings: list[str] = []
    for stream in data_streams:
        stream_name = str(stream.get("name", ""))
        rule = rules.get(stream_name)
        if rule is None:
            continue
        indices = stream.get("indices", [])
        write_index = str(indices[-1].get("index_name", ""))
        for entry in indices:
            index_name = str(entry.get("index_name", ""))
            if index_name == write_index:
                continue
            rollover_time_ms = rollover_times.get(index_name)
            if rollover_time_ms is None:
                raise ArchiveError(f"No verified rollover time for {index_name}")
            rollover_utc = dt.datetime.fromtimestamp(rollover_time_ms / 1000, tz=dt.timezone.utc)
            if rollover_utc > now.astimezone(dt.timezone.utc):
                raise ArchiveError(f"Backing index {index_name} has a future rollover date")
            rollover_local = rollover_utc.astimezone(timezone)
            age_days = (local_today - rollover_local.date()).days
            used_override = resolved_overrides.get(index_name)
            candidate = Candidate(
                index=index_name,
                index_uuid=str(entry.get("index_uuid", "")),
                data_stream=stream_name,
                rule=str(rule["name"]),
                rollover_time_ms=rollover_time_ms,
                rollover_local=rollover_local.isoformat(),
                age_days=age_days,
                rollover_time_source="override" if used_override else "cluster_state",
                rollover_override_reason=used_override.reason if used_override else "",
            )
            if not candidate.index_uuid:
                raise ArchiveError(f"Backing index {index_name} has no index_uuid")
            minimum = int(rule["min_age_days"])
            maximum = int(rule["max_age_days"])
            if minimum <= age_days <= maximum:
                candidates.append(candidate)
            elif age_days > maximum:
                overdue.append(candidate)

    candidates.sort(key=lambda item: (item.rollover_time_ms, item.index))
    overdue.sort(key=lambda item: (item.rollover_time_ms, item.index))
    limit = int(selection_config["max_indices_per_run"])
    if len(candidates) > limit:
        warnings.append(
            f"{len(candidates)} candidates found; only the oldest {limit} will be processed this run"
        )
        candidates = candidates[:limit]
    if overdue:
        warnings.append(
            f"{len(overdue)} indices are older than their configured maximum age and were not selected"
        )
    return Selection(candidates=candidates, overdue=overdue, warnings=warnings)


def discover_selection(
    client: ElasticsearchClient, selection_config: dict[str, Any], now: dt.datetime
) -> Selection:
    data_streams = get_data_streams(client)
    rules_by_stream, match_errors = match_stream_rules(data_streams, selection_config["rules"])
    if match_errors:
        raise ArchiveError("; ".join(match_errors))
    rollover_targets: list[tuple[str, str, str]] = []
    for stream in data_streams:
        stream_name = str(stream.get("name", ""))
        if stream_name not in rules_by_stream:
            continue
        indices = stream.get("indices", [])
        if not indices:
            raise ArchiveError(f"Data stream {stream_name} has no backing indices")
        write_index = str(indices[-1].get("index_name", ""))
        for entry in indices:
            index_name = str(entry.get("index_name", ""))
            if index_name and index_name != write_index:
                rollover_targets.append(
                    (index_name, stream_name, str(entry.get("index_uuid", "")))
                )
    overrides = parse_rollover_time_overrides(selection_config)
    rollover_times, used_overrides, override_warnings, rollover_errors = resolve_rollover_times(
        client, rollover_targets, overrides
    )
    if rollover_errors:
        raise ArchiveError("; ".join(rollover_errors))
    selection = select_candidates(
        data_streams, rollover_times, selection_config, now, used_overrides
    )
    selection.warnings.extend(override_warnings)
    return selection


def snapshot_name(prefix: str) -> str:
    stamp = utc_now().strftime("%Y%m%dt%H%M%Sz")
    return f"{prefix}-{stamp}-{uuid.uuid4().hex[:8]}".lower()


def get_snapshot(
    client: ElasticsearchClient, repository: str, name: str, allow_missing: bool = False
) -> dict[str, Any] | None:
    allowed = {404} if allow_missing else None
    status, response = client.request(
        "GET", f"/_snapshot/{quote_segment(repository)}/{quote_segment(name)}", allowed_status=allowed
    )
    if status == 404:
        return None
    snapshots = response.get("snapshots", [])
    if len(snapshots) != 1:
        raise ArchiveError(f"Expected exactly one snapshot named {name}, got {len(snapshots)}")
    return snapshots[0]


def validate_successful_snapshot(
    snapshot: dict[str, Any], expected_indices: list[str], expected_data_streams: list[str]
) -> None:
    state = snapshot.get("state")
    shards = snapshot.get("shards", {})
    failures = snapshot.get("failures", [])
    actual_indices = set(snapshot.get("indices", []))
    expected = set(expected_indices)
    actual_data_streams = set(snapshot.get("data_streams", []))
    expected_streams = set(expected_data_streams)
    if state != "SUCCESS":
        raise ArchiveError(f"Snapshot state is {state}, not SUCCESS")
    if int(shards.get("failed", -1)) != 0:
        raise ArchiveError(f"Snapshot has failed shards: {shards}")
    if int(shards.get("successful", -1)) != int(shards.get("total", -2)):
        raise ArchiveError(f"Snapshot shard counts do not match: {shards}")
    if failures:
        raise ArchiveError(f"Snapshot reports failures: {failures}")
    if not expected.issubset(actual_indices):
        missing = sorted(expected - actual_indices)
        raise ArchiveError(f"Snapshot does not contain every deletion candidate; missing={missing}")
    if actual_data_streams != expected_streams:
        missing = sorted(expected_streams - actual_data_streams)
        unexpected = sorted(actual_data_streams - expected_streams)
        raise ArchiveError(
            f"Snapshot data-stream set mismatch; missing={missing}, unexpected={unexpected}"
        )


def wait_for_snapshot(
    client: ElasticsearchClient,
    repository: str,
    name: str,
    poll_seconds: int,
    timeout_seconds: int,
    allow_initial_missing: bool,
    missing_grace_seconds: int = 60,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    missing_deadline = time.monotonic() + min(missing_grace_seconds, timeout_seconds)
    while time.monotonic() < deadline:
        snapshot = get_snapshot(client, repository, name, allow_missing=allow_initial_missing)
        if snapshot is None:
            if time.monotonic() >= missing_deadline:
                raise SnapshotMissingError(
                    f"Snapshot {name} was not found after the creation grace period"
                )
        else:
            state = str(snapshot.get("state", ""))
            if state in TERMINAL_SNAPSHOT_STATES:
                return snapshot
        time.sleep(poll_seconds)
    raise ArchiveError(f"Timed out waiting for snapshot {name}; transaction state was retained")


def assert_no_running_snapshot(client: ElasticsearchClient, repository: str) -> None:
    _, response = client.request("GET", f"/_snapshot/{quote_segment(repository)}/_status")
    running = response.get("snapshots", [])
    if running:
        names = [str(item.get("snapshot", "<unknown>")) for item in running]
        raise ArchiveError(f"Repository already has running snapshots: {', '.join(names)}")


def plan_expired_snapshots(
    snapshots: list[dict[str, Any]],
    rules: list[dict[str, Any]],
    prefix: str,
    now: dt.datetime,
    max_deletions: int,
) -> SnapshotCleanupPlan:
    if now.tzinfo is None:
        raise ValueError("Snapshot cleanup time must be timezone-aware")
    now_ms = int(now.timestamp() * 1000)
    expired: list[tuple[int, SnapshotExpiry]] = []
    warnings: list[str] = []

    for snapshot in snapshots:
        name = str(snapshot.get("snapshot", ""))
        snapshot_uuid = str(snapshot.get("uuid", ""))
        metadata = snapshot.get("metadata", {})
        if (
            not name.startswith(f"{prefix}-")
            or not isinstance(metadata, dict)
            or metadata.get("managed_by") != "elk-cold-archive"
            or snapshot.get("state") != "SUCCESS"
        ):
            continue
        if not snapshot_uuid:
            warnings.append(f"Snapshot cleanup skipped {name}: no snapshot UUID")
            continue
        data_streams = snapshot.get("data_streams", [])
        if not isinstance(data_streams, list) or not data_streams:
            warnings.append(f"Snapshot cleanup skipped {name}: no data streams")
            continue
        try:
            completed_ms = int(snapshot["end_time_in_millis"])
        except (KeyError, TypeError, ValueError):
            warnings.append(f"Snapshot cleanup skipped {name}: no valid success time")
            continue
        if completed_ms > now_ms:
            warnings.append(f"Snapshot cleanup skipped {name}: success time is in the future")
            continue

        has_recorded_retention = "retention_days" in metadata
        if has_recorded_retention:
            recorded_retention = metadata["retention_days"]
            if type(recorded_retention) is not int or recorded_retention < 0:
                warnings.append(
                    f"Snapshot cleanup skipped {name}: invalid recorded retention_days"
                )
                continue
            retention_days = recorded_retention

        matched_rules: dict[str, dict[str, Any]] = {}
        invalid_stream = False
        for stream in (str(value) for value in data_streams):
            matches = [
                rule
                for rule in rules
                if any(
                    fnmatch.fnmatchcase(stream, str(pattern))
                    for pattern in rule["data_stream_patterns"]
                )
            ]
            if len(matches) != 1:
                if has_recorded_retention:
                    continue
                warnings.append(
                    f"Snapshot cleanup skipped {name}: data stream {stream} matched "
                    f"{len(matches)} retention rules"
                )
                invalid_stream = True
                break
            matched_rules[str(matches[0]["name"])] = matches[0]
        if invalid_stream:
            continue

        if not has_recorded_retention:
            retention_days = max(
                oss_retention_days(rule) for rule in matched_rules.values()
            )
        cutoff_ms = now_ms - retention_days * 24 * 60 * 60 * 1000
        if completed_ms <= cutoff_ms:
            completed_at = dt.datetime.fromtimestamp(
                completed_ms / 1000, tz=dt.timezone.utc
            ).isoformat()
            expired.append(
                (
                    completed_ms,
                    SnapshotExpiry(
                        name=name,
                        uuid=snapshot_uuid,
                        archived_at=completed_at,
                        retention_days=retention_days,
                        data_streams=tuple(sorted(str(value) for value in data_streams)),
                        rules=tuple(sorted(matched_rules)),
                    ),
                )
            )

    expired.sort(key=lambda item: (item[0], item[1].name))
    if len(expired) > max_deletions:
        warnings.append(
            f"Snapshot cleanup limited this run to {max_deletions} of {len(expired)} expired snapshots"
        )
    return SnapshotCleanupPlan(
        expired=[item for _, item in expired[:max_deletions]], warnings=warnings
    )


def list_archive_snapshots(
    client: ElasticsearchClient, repository: str, prefix: str
) -> list[dict[str, Any]]:
    path = query_path(
        f"/_snapshot/{quote_segment(repository)}/{quote_segment(prefix + '-*')}",
        ignore_unavailable=True,
    )
    status, response = client.request("GET", path, allowed_status={404})
    if status == 404:
        return []
    snapshots = response.get("snapshots", [])
    if not isinstance(snapshots, list):
        raise ArchiveError("Unexpected snapshot-list response during cleanup")
    return snapshots


def cleanup_expired_snapshots(
    client: ElasticsearchClient,
    repository: str,
    prefix: str,
    rules: list[dict[str, Any]],
    cleanup_config: dict[str, Any],
    execute: bool,
    audit: AuditLog,
    summary: RunSummary,
    now: dt.datetime,
) -> None:
    assert_no_running_snapshot(client, repository)
    plan = plan_expired_snapshots(
        list_archive_snapshots(client, repository, prefix),
        rules,
        prefix,
        now,
        int(cleanup_config.get("max_deletions_per_run", 10)),
    )
    summary.expired_snapshots.extend(item.name for item in plan.expired)
    summary.warnings.extend(plan.warnings)
    audit.write(
        "snapshot_cleanup_plan",
        mode=summary.mode,
        expired=[item.to_dict() for item in plan.expired],
    )
    if not execute:
        return

    for item in plan.expired:
        current = get_snapshot(client, repository, item.name, allow_missing=True)
        if current is None:
            audit.write("expired_snapshot_already_absent", **item.to_dict())
            continue
        if str(current.get("uuid", "")) != item.uuid:
            raise ArchiveError(
                f"Snapshot UUID changed for {item.name}; refusing name-only delete"
            )
        confirmed = plan_expired_snapshots(
            [current],
            rules,
            prefix,
            now,
            max_deletions=1,
        )
        if not confirmed.expired or confirmed.expired[0].uuid != item.uuid:
            raise ArchiveError(f"Snapshot {item.name} is no longer eligible for cleanup")
        path = f"/_snapshot/{quote_segment(repository)}/{quote_segment(item.name)}"
        _, response = client.request("DELETE", path)
        if not isinstance(response, dict) or response.get("acknowledged") is not True:
            raise ArchiveError(
                f"Snapshot delete was not acknowledged for {item.name}: {response}"
            )
        if get_snapshot(client, repository, item.name, allow_missing=True) is not None:
            raise ArchiveError(f"Snapshot {item.name} still exists after acknowledged delete")
        summary.deleted_snapshots.append(item.name)
        audit.write("expired_snapshot_deleted", **item.to_dict())


def create_transaction(
    repository: str, snapshot: str, candidates: list[Candidate], cluster_uuid: str
) -> dict[str, Any]:
    if not cluster_uuid:
        raise ArchiveError("Cannot create a transaction without a cluster UUID")
    data_streams = sorted({candidate.data_stream for candidate in candidates})
    return {
        "schema_version": 2,
        "repository": repository,
        "snapshot": snapshot,
        "cluster_uuid": cluster_uuid,
        "data_streams": data_streams,
        "phase": "creating",
        "created_at": iso_now(),
        "indices": [candidate.to_dict() for candidate in candidates],
        "deleted": [],
    }


def start_snapshot(
    client: ElasticsearchClient,
    state_store: StateStore,
    audit: AuditLog,
    state: dict[str, Any],
) -> None:
    state_store.save(state)
    indices = [item["index"] for item in state["indices"]]
    data_streams = [str(value) for value in state["data_streams"]]
    metadata = {
        "managed_by": "elk-cold-archive",
        "created_at": state["created_at"],
    }
    if "retention_days" in state:
        metadata["retention_days"] = int(state["retention_days"])
    body = {
        # Elasticsearch snapshots data streams as a unit; individual backing indices
        # are not valid independent selection units for this API.
        "indices": ",".join(data_streams),
        "ignore_unavailable": False,
        "include_global_state": False,
        "partial": False,
        "metadata": metadata,
    }
    path = query_path(
        f"/_snapshot/{quote_segment(state['repository'])}/{quote_segment(state['snapshot'])}",
        wait_for_completion=False,
    )
    _, response = client.request("PUT", path, body=body)
    if not isinstance(response, dict) or response.get("accepted") is not True:
        raise ArchiveError(f"Snapshot creation was not accepted: {response}")
    state["phase"] = "waiting"
    state_store.save(state)
    audit.write(
        "snapshot_started",
        snapshot=state["snapshot"],
        data_streams=data_streams,
        deletion_candidates=indices,
        retention_days=state.get("retention_days"),
    )


def revalidate_delete_target(client: ElasticsearchClient, item: dict[str, Any]) -> str:
    """Return 'ready' or 'absent' after validating one target immediately before DELETE."""
    streams = get_data_streams(client)
    membership, membership_errors = build_membership(streams)
    if membership_errors:
        raise ArchiveError("; ".join(membership_errors))
    index_name = str(item["index"])
    expected_uuid = str(item.get("index_uuid", ""))
    if not expected_uuid:
        raise ArchiveError(f"Transaction has no expected index UUID for {index_name}")
    current = membership.get(index_name)
    if current is None:
        status, _ = client.request("HEAD", f"/{quote_segment(index_name)}", allowed_status={404})
        if status == 404:
            return "absent"
        raise ArchiveError(
            f"Index {index_name} still exists but is no longer attached to its expected data stream"
        )
    stream_name, index_uuid, is_write = current
    if stream_name != item["data_stream"]:
        raise ArchiveError(f"Index {index_name} moved from {item['data_stream']} to {stream_name}")
    if index_uuid != expected_uuid:
        raise ArchiveError(f"Index UUID changed for {index_name}; refusing name-only delete")
    if is_write:
        raise ArchiveError(f"Refusing to delete current write index {index_name}")
    return "ready"


def delete_verified_indices(
    client: ElasticsearchClient,
    state_store: StateStore,
    audit: AuditLog,
    state: dict[str, Any],
    summary: RunSummary,
) -> None:
    deleted = set(state.get("deleted", []))
    for item in state["indices"]:
        index_name = str(item["index"])
        if index_name in deleted:
            continue
        validation = revalidate_delete_target(client, item)
        if validation == "absent":
            state["deleted"].append(index_name)
            deleted.add(index_name)
            state_store.save(state)
            audit.write("index_already_absent", index=index_name, snapshot=state["snapshot"])
            continue
        # Elasticsearch has no UUID precondition for delete-index. The UUID check above is
        # therefore repeated per index immediately before this concrete-name DELETE.
        _, response = client.request("DELETE", f"/{quote_segment(index_name)}")
        if not isinstance(response, dict) or response.get("acknowledged") is not True:
            raise ArchiveError(f"Delete was not acknowledged for {index_name}: {response}")
        status, _ = client.request("HEAD", f"/{quote_segment(index_name)}", allowed_status={404})
        if status != 404:
            raise ArchiveError(f"Index {index_name} still exists after acknowledged delete")
        state["deleted"].append(index_name)
        deleted.add(index_name)
        state["phase"] = "deleting"
        state_store.save(state)
        summary.deleted.append(index_name)
        audit.write("index_deleted", index=index_name, snapshot=state["snapshot"])


def finish_transaction(
    state_store: StateStore, audit: AuditLog, state: dict[str, Any]
) -> None:
    expected = {item["index"] for item in state["indices"]}
    if set(state["deleted"]) != expected:
        raise ArchiveError("Transaction cannot complete because not all indices are accounted for")
    state["phase"] = "complete"
    state["completed_at"] = iso_now()
    state_store.save(state)
    audit.write("transaction_complete", snapshot=state["snapshot"], deleted=state["deleted"])
    state_store.clear()


def complete_or_resume_transaction(
    client: ElasticsearchClient,
    execution: dict[str, Any],
    state_store: StateStore,
    audit: AuditLog,
    state: dict[str, Any],
    summary: RunSummary,
) -> None:
    state_cluster_uuid = str(state.get("cluster_uuid", ""))
    if not state_cluster_uuid or state_cluster_uuid != summary.cluster_uuid:
        raise ArchiveError(
            f"Transaction cluster UUID {state_cluster_uuid or '<missing>'} does not match "
            f"connected cluster {summary.cluster_uuid or '<missing>'}"
        )
    expected_indices = [item["index"] for item in state["indices"]]
    expected_data_streams = [str(value) for value in state.get("data_streams", [])]
    if not expected_data_streams:
        raise ArchiveError("Transaction has no snapshotted data streams")
    try:
        snapshot = wait_for_snapshot(
            client,
            state["repository"],
            state["snapshot"],
            int(execution.get("snapshot_poll_seconds", 15)),
            int(execution.get("snapshot_timeout_seconds", 21600)),
            allow_initial_missing=True,
            missing_grace_seconds=int(execution.get("snapshot_missing_grace_seconds", 60)),
        )
    except SnapshotMissingError:
        failed_path = state_store.archive_failed(state)
        audit.write(
            "snapshot_missing",
            snapshot=state["snapshot"],
            retained_state=str(failed_path),
        )
        raise
    if snapshot.get("state") != "SUCCESS":
        failed_path = state_store.archive_failed(state)
        audit.write(
            "snapshot_failed",
            snapshot=state["snapshot"],
            state=snapshot.get("state"),
            retained_state=str(failed_path),
        )
        raise ArchiveError(
            f"Snapshot {state['snapshot']} ended as {snapshot.get('state')}; no index was deleted"
        )
    validate_successful_snapshot(snapshot, expected_indices, expected_data_streams)
    state["phase"] = "verified"
    state["verified_at"] = iso_now()
    state_store.save(state)
    audit.write("snapshot_verified", snapshot=state["snapshot"], indices=expected_indices)
    if state["snapshot"] not in summary.snapshots:
        summary.snapshots.append(state["snapshot"])

    # Re-read and revalidate the immutable snapshot immediately before deletion.
    confirmed = get_snapshot(client, state["repository"], state["snapshot"])
    if confirmed is None:
        raise ArchiveError(f"Verified snapshot {state['snapshot']} disappeared")
    validate_successful_snapshot(confirmed, expected_indices, expected_data_streams)
    delete_verified_indices(client, state_store, audit, state, summary)
    finish_transaction(state_store, audit, state)


def run_archive(
    client: ElasticsearchClient,
    config: dict[str, Any],
    execute: bool,
    summary: RunSummary,
    audit: AuditLog,
    state_store: StateStore,
) -> None:
    run_deadline = time.monotonic() + int(config["execution"].get("max_run_seconds", 82800))
    preflight(client, config, execute, summary)
    execution = config["execution"]
    repository = str(config["elasticsearch"]["repository"])

    pending = state_store.load()
    if pending:
        if pending.get("repository") != repository:
            raise ArchiveError("Pending transaction uses a different snapshot repository")
        summary.warnings.append(f"Pending transaction found: {pending.get('snapshot')}")
        if not execute:
            summary.warnings.append("Dry-run did not resume or delete the pending transaction")
            return
        complete_or_resume_transaction(client, execution, state_store, audit, pending, summary)

    selection = discover_selection(client, config["selection"], utc_now())
    summary.candidates.extend(candidate.index for candidate in selection.candidates)
    summary.overdue.extend(candidate.index for candidate in selection.overdue)
    summary.warnings.extend(selection.warnings)
    for candidate in selection.candidates:
        LOG.info(
            "candidate index=%s data_stream=%s rollover=%s age_days=%d rule=%s "
            "rollover_source=%s",
            candidate.index,
            candidate.data_stream,
            candidate.rollover_local,
            candidate.age_days,
            candidate.rule,
            candidate.rollover_time_source,
        )
    for candidate in selection.overdue:
        LOG.warning(
            "overdue index=%s data_stream=%s rollover=%s age_days=%d rule=%s",
            candidate.index,
            candidate.data_stream,
            candidate.rollover_local,
            candidate.age_days,
            candidate.rule,
        )

    audit.write(
        "plan",
        mode=summary.mode,
        candidates=[candidate.to_dict() for candidate in selection.candidates],
        overdue=[candidate.to_dict() for candidate in selection.overdue],
        warnings=selection.warnings,
    )
    prefix = str(config["elasticsearch"].get("snapshot_prefix", "cold-archive"))
    if execute and selection.candidates:
        batch_size = int(config["selection"]["max_data_streams_per_snapshot"])
        groups = group_candidates_by_retention(
            selection.candidates, config["selection"]["rules"]
        )
        budget_exhausted = False
        for retention_days, retention_candidates in groups:
            retention_streams = list(
                dict.fromkeys(candidate.data_stream for candidate in retention_candidates)
            )
            for stream_batch in chunked(retention_streams, batch_size):
                remaining_seconds = int(run_deadline - time.monotonic())
                if remaining_seconds < 60:
                    summary.warnings.append(
                        "Run time budget was exhausted before all candidate data streams were processed"
                    )
                    budget_exhausted = True
                    break
                stream_names = set(stream_batch)
                batch = [
                    candidate
                    for candidate in retention_candidates
                    if candidate.data_stream in stream_names
                ]
                assert_no_running_snapshot(client, repository)
                name = snapshot_name(prefix)
                state = create_transaction(repository, name, batch, summary.cluster_uuid)
                state["retention_days"] = retention_days
                start_snapshot(client, state_store, audit, state)
                bounded_execution = dict(execution)
                bounded_execution["snapshot_timeout_seconds"] = min(
                    int(execution.get("snapshot_timeout_seconds", 21600)),
                    remaining_seconds,
                )
                complete_or_resume_transaction(
                    client, bounded_execution, state_store, audit, state, summary
                )
            if budget_exhausted:
                break

    cleanup = config.get("snapshot_cleanup", {})
    if cleanup.get("enabled", False):
        if time.monotonic() >= run_deadline:
            summary.warnings.append("Snapshot cleanup skipped because the run time budget expired")
        else:
            try:
                cleanup_expired_snapshots(
                    client,
                    repository,
                    prefix,
                    config["selection"]["rules"],
                    cleanup,
                    execute,
                    audit,
                    summary,
                    utc_now(),
                )
            except ArchiveError as exc:
                summary.warnings.append(
                    f"Snapshot cleanup failed without affecting archive transactions: {exc}"
                )
                audit.write("snapshot_cleanup_failed", error=str(exc))


@contextlib.contextmanager
def smtp_session(mail_config: dict[str, Any]) -> Iterable[smtplib.SMTP]:
    host = str(require(mail_config, "host", "mail"))
    port = int(require(mail_config, "port", "mail"))
    if not host:
        raise ConfigError("mail.host is required when mail is enabled")
    username = os.environ.get(str(mail_config.get("username_env", "SMTP_USERNAME")), "").strip()
    if not username:
        username = str(mail_config.get("username", "")).strip()
    password = read_secret(
        str(mail_config.get("password_env", "SMTP_PASSWORD")),
        str(mail_config.get("password_file_env", "SMTP_PASSWORD_FILE")),
        "smtp_password",
    )
    if username and not password:
        raise ConfigError("SMTP username is configured, but SMTP_PASSWORD or SMTP_PASSWORD_FILE is empty")

    context = ssl.create_default_context()
    security = str(mail_config.get("security", "starttls")).lower()
    timeout = float(mail_config.get("timeout_seconds", 30))
    if security == "ssl":
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as smtp:
            if username:
                smtp.login(username, password)
            yield smtp
    elif security == "starttls":
        with smtplib.SMTP(host, port, timeout=timeout) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            if username:
                smtp.login(username, password)
            yield smtp
    else:
        raise ConfigError("mail.security must be 'ssl' or 'starttls'")


def verify_mail_ready(mail_config: dict[str, Any]) -> None:
    if not mail_config.get("enabled", False):
        raise ConfigError(
            "Live mode requires mail.enabled=true unless require_ready_before_delete is explicitly false"
        )
    sender = str(require(mail_config, "from", "mail")).strip()
    recipients = mail_config.get("to", [])
    if isinstance(recipients, str):
        recipients = [recipients]
    if not sender or not recipients or any(not str(value).strip() for value in recipients):
        raise ConfigError("mail.from and at least one non-empty mail.to recipient are required")
    with smtp_session(mail_config) as smtp:
        code, response = smtp.noop()
        if not 200 <= code < 400:
            raise ArchiveError(f"SMTP readiness check failed: {code} {response!r}")


def send_mail(mail_config: dict[str, Any], summary: RunSummary) -> None:
    if not mail_config.get("enabled", False):
        return
    sender = str(require(mail_config, "from", "mail"))
    recipients = mail_config.get("to", [])
    if isinstance(recipients, str):
        recipients = [recipients]
    if not sender or not recipients:
        raise ConfigError("mail.from and mail.to are required when mail is enabled")

    outcome = "FAILED" if summary.errors else "WARNING" if summary.warnings else "OK"
    message = email.message.EmailMessage()
    message["Subject"] = f"[ELK cold archive] {summary.mode} {outcome}"
    message["From"] = sender
    message["To"] = ", ".join(str(value) for value in recipients)
    message.set_content(summary.render())
    with smtp_session(mail_config) as smtp:
        smtp.send_message(message)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=pathlib.Path, help="JSON configuration path")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Create snapshots and delete verified indices; omitted means dry-run",
    )
    parser.add_argument("--no-mail", action="store_true", help="Do not send the run summary email")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--version", action="version", version=VERSION)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    summary = RunSummary(mode="LIVE" if args.execute else "DRY-RUN", started_at=iso_now())
    config: dict[str, Any] | None = None
    exit_code = 0
    try:
        if args.execute and args.no_mail:
            raise ConfigError("--no-mail is allowed only in dry-run mode")
        config = load_json(args.config)
        validate_config(config)
        execution = config["execution"]
        audit = AuditLog(pathlib.Path(execution["audit_log"]))
        state_store = StateStore(pathlib.Path(execution["state_file"]))
        client = ElasticsearchClient(config["elasticsearch"])
        if args.execute:
            LOG.warning("LIVE MODE: successful snapshots will be followed by exact-index deletes")
        with exclusive_lock(pathlib.Path(execution["lock_file"])):
            mail_config = config.get("mail", {})
            if args.execute and mail_config.get("require_ready_before_delete", True):
                verify_mail_ready(mail_config)
            run_archive(client, config, args.execute, summary, audit, state_store)
    except Exception as exc:  # The top-level guard must also report unexpected failures.
        LOG.exception("Archive run failed")
        summary.errors.append(str(exc))
        exit_code = 1
    finally:
        summary.finished_at = iso_now()
        LOG.info("\n%s", summary.render())
        if config is not None and not args.no_mail:
            try:
                send_mail(config.get("mail", {}), summary)
            except Exception as exc:
                LOG.exception("Could not send summary email")
                if not summary.errors:
                    summary.errors.append(f"Email delivery failed: {exc}")
                    exit_code = 3
        if summary.warnings and exit_code == 0:
            exit_code = 2
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
