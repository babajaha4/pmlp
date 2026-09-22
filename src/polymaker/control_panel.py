"""Loopback-only web control panel backed by the user's Bitvise SSH profile."""

from __future__ import annotations

import base64
import gzip
import hmac
import ipaddress
import json
import re
import secrets
import shlex
import subprocess
import threading
import webbrowser
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_SNAPSHOT_BEGIN = "POLYMAKER_SNAPSHOT_BEGIN"
_SNAPSHOT_END = "POLYMAKER_SNAPSHOT_END"
_SUCCESS_MARKER = "REMOTE_VERIFY_EXIT=0"
_SAFE_UNIT = re.compile(r"[A-Za-z0-9_.@-]+")
_SAFE_REMOTE_PATH = re.compile(r"/[A-Za-z0-9_./-]+")
_SAFE_CONFIG_DIR = re.compile(r"[A-Za-z0-9_./-]+")
_SENSITIVE_HEX = re.compile(r"0x[0-9a-fA-F]{20,}")
_SENSITIVE_NUMBER = re.compile(r"\b[0-9]{30,}\b")
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_ -]?key|secret|passphrase|private[_ -]?key|pk)\s*[:=]\s*\S+"
)


class RemoteCommandError(RuntimeError):
    """A fixed remote control command failed or returned incomplete output."""


@dataclass(frozen=True, slots=True)
class ControlSettings:
    sexec: Path
    profile: Path
    remote_dir: str = "/home/ubuntu/pmlp"
    remote_config_dir: str = "livecfg"
    unit: str = "polymaker-live.service"
    timeout_seconds: float = 90.0

    def validate(self) -> None:
        if not self.sexec.is_file():
            raise ValueError(f"Bitvise sexec.exe not found: {self.sexec}")
        if not self.profile.is_file():
            raise ValueError(f"Bitvise profile not found: {self.profile}")
        if _SAFE_REMOTE_PATH.fullmatch(self.remote_dir) is None or ".." in self.remote_dir:
            raise ValueError("remote directory must be an absolute safe path")
        if (
            _SAFE_CONFIG_DIR.fullmatch(self.remote_config_dir) is None
            or self.remote_config_dir.startswith("/")
            or ".." in self.remote_config_dir
        ):
            raise ValueError("remote config directory must be a safe relative path")
        if _SAFE_UNIT.fullmatch(self.unit) is None:
            raise ValueError("invalid systemd unit name")
        if self.timeout_seconds < 10:
            raise ValueError("SSH timeout must be at least 10 seconds")


class BitviseControl:
    """Execute a small allowlist of fixed operational commands over SSH."""

    def __init__(self, settings: ControlSettings) -> None:
        settings.validate()
        self.settings = settings
        self._operation_lock = threading.Lock()

    def _invoke(self, command: str) -> str:
        result = subprocess.run(
            [
                str(self.settings.sexec),
                f"-profile={self.settings.profile}",
                f"-cmd={command}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=self.settings.timeout_seconds,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        output = "\n".join(part for part in (result.stdout, result.stderr) if part)
        if _SUCCESS_MARKER not in output:
            safe = _sanitize_remote_output(output)
            detail = safe[-1200:].strip() or f"Bitvise exit code {result.returncode}"
            raise RemoteCommandError(detail)
        return output

    def snapshot(self) -> dict[str, Any]:
        with self._operation_lock:
            return self._snapshot_unlocked()

    def perform(self, action: str) -> dict[str, Any]:
        if action not in {"start", "stop", "restart"}:
            raise ValueError("unsupported service action")
        with self._operation_lock:
            self._invoke(self._action_command(action))
            output = self._invoke(self._status_command(action == "stop"))
            values = _key_values(output)
            snapshot = self._snapshot_unlocked()
            if action == "stop":
                if int(snapshot["summary"]["managed_order_count"]) != 0:
                    raise RemoteCommandError(
                        "service stopped but configured-token orders remain on the exchange"
                    )
            elif (
                snapshot["service"]["active_state"] != "active"
                or not snapshot["summary"]["ledger_matches_positions"]
                or int(snapshot["health"]["state_unknown"]) != 0
            ):
                raise RemoteCommandError("service did not pass the authoritative startup snapshot")
            return {
                "action": action,
                "active_state": values.get("ActiveState", "unknown"),
                "sub_state": values.get("SubState", "unknown"),
                "main_pid": int(values.get("MainPID", "0") or 0),
                "restarts": int(values.get("NRestarts", "0") or 0),
                "process_count": int(values.get("POLYMAKER_RUN_PROCESS_COUNT", "0") or 0),
                "snapshot": snapshot,
            }

    def _snapshot_unlocked(self) -> dict[str, Any]:
        root = shlex.quote(self.settings.remote_dir)
        config = shlex.quote(self.settings.remote_config_dir)
        source = Path(__file__).with_name("monitoring.py").read_bytes()
        encoded = base64.b64encode(gzip.compress(source, compresslevel=9)).decode("ascii")
        bootstrap = (
            "import asyncio,base64,gzip,json;"
            "ns={'__name__':'polymaker.monitoring_remote'};"
            f"exec(gzip.decompress(base64.b64decode('{encoded}')),ns);"
            "from polymaker.config import Config;"
            f"snapshot=asyncio.run(ns['collect_live_snapshot'](Config.load({config!r}),"
            f"unit={self.settings.unit!r}));"
            f"print('{_SNAPSHOT_BEGIN}');"
            "print(json.dumps(snapshot,separators=(',',':'),sort_keys=True));"
            f"print('{_SNAPSHOT_END}')"
        )
        command = (
            f"set -e; cd {root}; "
            f".venv/bin/python -c {shlex.quote(bootstrap)}; "
            "printf 'REMOTE_VERIFY_EXIT=0\\n'"
        )
        return _extract_snapshot(self._invoke(command))

    def _action_command(self, action: str) -> str:
        settings = self.settings
        unit = shlex.quote(settings.unit)
        root = shlex.quote(settings.remote_dir)
        config = shlex.quote(settings.remote_config_dir)
        binary = shlex.quote(f"{settings.remote_dir}/.venv/bin/polymaker")
        launch = (
            f"sudo systemd-run --unit={unit} --property=Restart=on-failure "
            f"--property=KillSignal=SIGINT --property=User=ubuntu "
            f"--property=TimeoutStopSec=45s "
            f"--working-directory={root} --no-block {binary} run "
            f"--config-dir {config} --live --confirm-live"
        )
        wait_active = (
            f"i=0; until systemctl is-active --quiet {unit}; do "
            "i=$((i+1)); test \"$i\" -lt 20; sleep 1; done"
        )
        if action == "stop":
            return (
                "set -e; "
                f"if systemctl cat {unit} >/dev/null 2>&1; then sudo systemctl stop {unit}; fi; "
                f"! systemctl is-active --quiet {unit}; "
                "printf 'REMOTE_VERIFY_EXIT=0\\n'"
            )
        if action == "restart":
            operation = (
                f"if systemctl is-active --quiet {unit}; then sudo systemctl restart {unit}; "
                f"else {launch}; fi"
            )
        else:
            operation = (
                f"if ! systemctl is-active --quiet {unit}; then {launch}; fi"
            )
        return (
            f"set -e; cd {root}; {operation}; {wait_active}; sleep 3; "
            "printf 'REMOTE_VERIFY_EXIT=0\\n'"
        )

    def _status_command(self, expect_stopped: bool) -> str:
        settings = self.settings
        unit = shlex.quote(settings.unit)
        process_pattern = shlex.quote(
            f"[p]olymaker run --config-dir {settings.remote_config_dir} --live --confirm-live"
        )
        expected = 0 if expect_stopped else 1
        return (
            "set -e; "
            f"if systemctl cat {unit} >/dev/null 2>&1; then "
            f"systemctl show {unit} -p ActiveState -p SubState -p MainPID -p NRestarts; "
            "else printf 'ActiveState=inactive\\nSubState=dead\\nMainPID=0\\nNRestarts=0\\n'; fi; "
            f"proc_count=$(pgrep -fc {process_pattern} || true); "
            "printf 'POLYMAKER_RUN_PROCESS_COUNT=%s\\n' \"$proc_count\"; "
            f"test \"$proc_count\" -eq {expected}; "
            "printf 'REMOTE_VERIFY_EXIT=0\\n'"
        )


def _extract_snapshot(output: str) -> dict[str, Any]:
    start = output.rfind(_SNAPSHOT_BEGIN)
    end = output.rfind(_SNAPSHOT_END)
    if start < 0 or end <= start:
        raise RemoteCommandError("remote snapshot markers are missing")
    payload = output[start + len(_SNAPSHOT_BEGIN):end].strip()
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RemoteCommandError("remote snapshot JSON is invalid") from exc
    if not isinstance(decoded, dict):
        raise RemoteCommandError("remote snapshot is not an object")
    return decoded


def _key_values(output: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in output.splitlines():
        key, separator, value = line.strip().partition("=")
        if separator and re.fullmatch(r"[A-Za-z_]+", key):
            values[key] = value
    return values


def _sanitize_remote_output(output: str) -> str:
    output = _SENSITIVE_ASSIGNMENT.sub(r"\1=<redacted>", output)
    output = _SENSITIVE_HEX.sub("<redacted-address>", output)
    return _SENSITIVE_NUMBER.sub("<redacted-token>", output)


class ControlPanelApplication:
    def __init__(self, remote: BitviseControl, host: str, port: int, poll_seconds: int) -> None:
        self.remote = remote
        self.host = host
        self.port = port
        self.poll_seconds = poll_seconds
        self.control_token = secrets.token_urlsafe(32)
        self.origins = {f"http://{host}:{port}", f"http://localhost:{port}"}

    def page(self) -> bytes:
        path = Path(__file__).with_name("control_ui") / "index.html"
        content = path.read_text(encoding="utf-8")
        content = content.replace("__CONTROL_TOKEN__", self.control_token)
        content = content.replace("__POLL_SECONDS__", str(self.poll_seconds))
        return content.encode()

    @staticmethod
    def asset(name: str) -> tuple[bytes, str]:
        if name not in {"app.css", "app.js"}:
            raise FileNotFoundError(name)
        path = Path(__file__).with_name("control_ui") / name
        content_type = "text/css; charset=utf-8" if name.endswith(".css") else (
            "text/javascript; charset=utf-8"
        )
        return path.read_bytes(), content_type

    def valid_host(self, value: str | None) -> bool:
        if not value:
            return False
        hostname = value.rsplit(":", 1)[0].strip("[]")
        return hostname in {self.host, "localhost"}

    def valid_origin(self, value: str | None) -> bool:
        return value in self.origins


class _ControlServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        application: ControlPanelApplication,
    ) -> None:
        self.application = application
        super().__init__(server_address, handler)


class ControlRequestHandler(BaseHTTPRequestHandler):
    server: _ControlServer

    def log_message(self, format: str, *args: object) -> None:
        return

    def _headers(self, status: HTTPStatus, content_type: str, length: int) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'",
        )
        self.end_headers()

    def _send(self, body: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self._headers(status, content_type, len(body))
        self.wfile.write(body)

    def _json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        self._send(
            json.dumps(payload, ensure_ascii=False).encode(),
            "application/json; charset=utf-8",
            status,
        )

    def _request_allowed(self) -> bool:
        return self.server.application.valid_host(self.headers.get("Host"))

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if not self._request_allowed():
            self._json({"error": "invalid host"}, HTTPStatus.FORBIDDEN)
            return
        path = urlparse(self.path).path
        try:
            if path == "/":
                self._send(self.server.application.page(), "text/html; charset=utf-8")
            elif path == "/assets/app.css":
                body, content_type = self.server.application.asset("app.css")
                self._send(body, content_type)
            elif path == "/assets/app.js":
                body, content_type = self.server.application.asset("app.js")
                self._send(body, content_type)
            elif path == "/api/snapshot":
                self._json({"ok": True, "data": self.server.application.remote.snapshot()})
            else:
                self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
        except (OSError, RemoteCommandError, subprocess.TimeoutExpired) as exc:
            self._json({"ok": False, "error": str(exc)}, HTTPStatus.BAD_GATEWAY)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        app = self.server.application
        if not self._request_allowed() or not app.valid_origin(self.headers.get("Origin")):
            self._json({"error": "request origin rejected"}, HTTPStatus.FORBIDDEN)
            return
        if not hmac.compare_digest(self.headers.get("X-Control-Token", ""), app.control_token):
            self._json({"error": "invalid control token"}, HTTPStatus.FORBIDDEN)
            return
        if urlparse(self.path).path != "/api/action":
            self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 4096:
                raise ValueError("invalid request length")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict) or body.get("action") not in {"start", "stop", "restart"}:
                raise ValueError("invalid action")
            result = app.remote.perform(str(body["action"]))
            self._json({"ok": True, "result": result})
        except ValueError as exc:
            self._json({"ok": False, "error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except (OSError, RemoteCommandError, subprocess.TimeoutExpired) as exc:
            self._json({"ok": False, "error": str(exc)}, HTTPStatus.BAD_GATEWAY)


def serve_control_panel(
    settings: ControlSettings,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    poll_seconds: int = 60,
    open_browser: bool = True,
) -> None:
    try:
        if not ipaddress.ip_address(host).is_loopback:
            raise ValueError("control panel must bind to a loopback address")
    except ValueError as exc:
        raise ValueError("control panel host must be a loopback IP address") from exc
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    if not 5 <= poll_seconds <= 300:
        raise ValueError("poll interval must be between 5 and 300 seconds")

    application = ControlPanelApplication(
        BitviseControl(settings), host=host, port=port, poll_seconds=poll_seconds
    )
    server = _ControlServer((host, port), ControlRequestHandler, application)
    url = f"http://{host}:{port}"
    if open_browser:
        threading.Timer(0.5, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
