from __future__ import annotations

import http.client
import json
import subprocess
import threading
from pathlib import Path
from typing import Any

import pytest

from polymaker.catalog.store import CatalogStore
from polymaker.config import Config, MarketEntry, PathsConfig, RiskConfig, StrategyProfile
from polymaker.control_panel import (
    BitviseControl,
    ControlPanelApplication,
    ControlRequestHandler,
    ControlSettings,
    RemoteCommandError,
    _ControlServer,
    _extract_snapshot,
    _sanitize_remote_output,
    serve_control_panel,
)
from polymaker.domain import Fill, OpenOrder, OrderState, Side
from polymaker.monitoring import collect_live_snapshot
from polymaker.state.store import StateStore


class FakeGateway:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def open_orders(self) -> list[OpenOrder]:
        return [
            OpenOrder("managed", "yes-token", Side.BUY, 0.4, 10, OrderState.LIVE),
            OpenOrder("manual", "other-token", Side.BUY, 0.2, 3, OrderState.LIVE),
        ]

    async def positions(self) -> dict[str, tuple[float, float]]:
        return {"yes-token": (10.0, 0.35)}

    async def collateral_balance(self) -> float:
        return 42.5

    async def get_book(self, token_id: str) -> dict[str, float]:
        if token_id == "yes-token":
            return {"best_bid": 0.39, "best_ask": 0.41, "bid_depth": 10, "ask_depth": 10}
        return {"best_bid": 0.59, "best_ask": 0.61, "bid_depth": 10, "ask_depth": 10}

    def close(self) -> None:
        self.closed = True


class FakeRemote:
    def __init__(self) -> None:
        self.actions: list[str] = []

    def snapshot(self) -> dict[str, Any]:
        return {"service": {"active_state": "active"}}

    def perform(self, action: str) -> dict[str, Any]:
        self.actions.append(action)
        return {"action": action, "active_state": "active"}


def _settings(tmp_path: Path) -> ControlSettings:
    sexec = tmp_path / "sexec.exe"
    profile = tmp_path / "profile.tlp"
    sexec.touch()
    profile.touch()
    return ControlSettings(sexec=sexec, profile=profile)


def test_control_settings_reject_shell_metacharacters(tmp_path):
    settings = _settings(tmp_path)
    with pytest.raises(ValueError, match="remote directory"):
        ControlSettings(
            sexec=settings.sexec,
            profile=settings.profile,
            remote_dir="/home/ubuntu/pmlp; whoami",
        ).validate()
    with pytest.raises(ValueError, match="config directory"):
        ControlSettings(
            sexec=settings.sexec,
            profile=settings.profile,
            remote_config_dir="../secrets",
        ).validate()


@pytest.mark.parametrize("action", ["start", "stop", "restart"])
def test_service_actions_are_scoped_and_never_use_dangerous_trading_commands(tmp_path, action):
    control = BitviseControl(_settings(tmp_path))
    command = control._action_command(action)

    assert "polymaker-live.service" in command
    assert "cancel-all" not in command
    assert " market" not in command
    assert "merge" not in command
    assert "--live --confirm-live" in command or action == "stop"
    if action == "stop":
        assert "systemctl stop" in command
        assert "polymaker halt" not in command
    else:
        assert "systemd-run" in command
        assert "KillSignal=SIGINT" in command


@pytest.mark.parametrize(("stopped", "expected"), [(False, "1"), (True, "0")])
def test_process_verification_runs_separately_from_launch_text(tmp_path, stopped, expected):
    control = BitviseControl(_settings(tmp_path))
    command = control._status_command(stopped)
    assert "systemd-run" not in command
    assert "--working-directory" not in command
    assert "[p]olymaker run" in command
    assert f'test "$proc_count" -eq {expected}' in command


def test_bitvise_remote_success_marker_overrides_wrapper_exit_code(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    control = BitviseControl(settings)

    def run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args[0], 1, stdout="REMOTE_VERIFY_EXIT=0\n", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    assert control._invoke("fixed command").strip() == "REMOTE_VERIFY_EXIT=0"


def test_bitvise_remote_failure_redacts_addresses_and_token_ids(tmp_path, monkeypatch):
    control = BitviseControl(_settings(tmp_path))

    def run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        output = "wallet=0x1234567890abcdef1234567890abcdef12345678 token=123456789012345678901234567890"
        return subprocess.CompletedProcess(args[0], 1, stdout=output, stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RemoteCommandError) as error:
        control._invoke("fixed command")
    assert "1234567890abcdef" not in str(error.value)
    assert "123456789012345678901234567890" not in str(error.value)


def test_snapshot_marker_parser_ignores_gateway_noise():
    output = (
        "gateway log\nPOLYMAKER_SNAPSHOT_BEGIN\n"
        '{"service":{"active_state":"active"}}\n'
        "POLYMAKER_SNAPSHOT_END\nREMOTE_VERIFY_EXIT=0\n"
    )
    assert _extract_snapshot(output)["service"]["active_state"] == "active"
    with pytest.raises(RemoteCommandError, match="markers"):
        _extract_snapshot("{}")


def test_remote_output_sanitizer():
    assert _sanitize_remote_output("0x" + "a" * 40) == "<redacted-address>"
    assert _sanitize_remote_output("9" * 40) == "<redacted-token>"
    assert _sanitize_remote_output("API_KEY=do-not-show") == "API_KEY=<redacted>"


def test_stop_requires_authoritative_managed_order_cleanup(tmp_path, monkeypatch):
    control = BitviseControl(_settings(tmp_path))
    monkeypatch.setattr(
        control,
        "_invoke",
        lambda command: (
            "ActiveState=inactive\nSubState=dead\nMainPID=0\nNRestarts=0\n"
            "POLYMAKER_RUN_PROCESS_COUNT=0\nREMOTE_VERIFY_EXIT=0"
        ),
    )
    monkeypatch.setattr(
        control,
        "_snapshot_unlocked",
        lambda: {
            "summary": {"managed_order_count": 1, "ledger_matches_positions": True},
            "service": {"active_state": "inactive"},
            "health": {"state_unknown": 0},
        },
    )
    with pytest.raises(RemoteCommandError, match="orders remain"):
        control.perform("stop")


def test_start_requires_authoritative_ledger_and_state_check(tmp_path, monkeypatch):
    control = BitviseControl(_settings(tmp_path))
    monkeypatch.setattr(
        control,
        "_invoke",
        lambda command: (
            "ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\n"
            "POLYMAKER_RUN_PROCESS_COUNT=1\nREMOTE_VERIFY_EXIT=0"
        ),
    )
    monkeypatch.setattr(
        control,
        "_snapshot_unlocked",
        lambda: {
            "summary": {"managed_order_count": 2, "ledger_matches_positions": False},
            "service": {"active_state": "active"},
            "health": {"state_unknown": 0},
        },
    )
    with pytest.raises(RemoteCommandError, match="startup snapshot"):
        control.perform("start")


async def test_read_only_snapshot_reports_exchange_ledger_risk_and_strategy(tmp_path, meta):
    db = tmp_path / "state.db"
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    catalog = CatalogStore(db)
    catalog.upsert_market(meta)
    catalog.close()
    state = StateStore(db)
    state.apply_fill(Fill("yes-token", Side.BUY, 0.35, 10, "fill-1", is_maker=True))
    state.save_risk_state(
        "2099-01-01",
        day_start_equity=0.25,
        net_cash=-3.5,
        daily_pnl=0.25,
        killed=False,
        manual_killed=False,
        order_attempts=8,
        order_errors=0,
    )
    state.record_pnl(0.5, -3.5, 4.0, 0.25)
    state.close()
    (log_dir / "live.jsonl").write_text(
        json.dumps({
            "cid": "0xcond",
            "regime": "QUIET",
            "fv": 0.4,
            "tox": 0.0,
            "flowz": 0.0,
            "event": "requote",
            "timestamp": "2099-01-01T00:00:01Z",
        }) + "\n",
        encoding="utf-8",
    )
    cfg = Config(
        risk=RiskConfig(
            max_total_exposure_usdc=40,
            max_event_group_loss_usdc=30,
            max_market_notional_usdc=15,
            daily_loss_kill_usdc=12,
        ),
        paths=PathsConfig(db=str(db), log_dir=str(log_dir), journal_dir=str(tmp_path / "journal")),
        profiles={"tiny": StrategyProfile(base_size_usdc=5, q_max_usdc=12, layers=1)},
        markets=[MarketEntry(slug=meta.slug, profile="tiny")],
    )
    gateway = FakeGateway()
    before = db.stat().st_mtime_ns

    snapshot = await collect_live_snapshot(
        cfg,
        gateway=gateway,
        service_status={
            "load_state": "loaded",
            "active_state": "active",
            "sub_state": "running",
            "main_pid": 123,
            "restarts": 0,
            "active_since": "",
        },
    )

    assert gateway.connected
    assert not gateway.closed  # caller-owned gateways stay open
    assert db.stat().st_mtime_ns == before
    assert snapshot["wallet"]["collateral_pusd"] == 42.5
    assert snapshot["summary"]["open_order_count"] == 2
    assert snapshot["summary"]["managed_order_count"] == 1
    assert snapshot["summary"]["unconfigured_order_count"] == 1
    assert snapshot["summary"]["buy_reservation"] == pytest.approx(4.0)
    assert snapshot["summary"]["inventory_value"] == pytest.approx(4.0)
    assert snapshot["summary"]["total_exposure"] == pytest.approx(8.0)
    assert snapshot["summary"]["ledger_matches_positions"]
    assert snapshot["markets"][0]["regime"] == "QUIET"
    assert snapshot["fills"]["all"]["maker_count"] == 1
    assert snapshot["strategy"]["maker_only"]
    assert snapshot["strategy"]["profiles"][0]["profile"] == "tiny"


async def test_true_empty_exchange_snapshot_remains_healthy(tmp_path, meta):
    class EmptyGateway(FakeGateway):
        async def open_orders(self) -> list[OpenOrder]:
            return []

        async def positions(self) -> dict[str, tuple[float, float]]:
            return {}

    db = tmp_path / "empty.db"
    catalog = CatalogStore(db)
    catalog.upsert_market(meta)
    catalog.close()
    state = StateStore(db)
    state.close()
    cfg = Config(
        paths=PathsConfig(db=str(db), log_dir=str(tmp_path), journal_dir=str(tmp_path)),
        profiles={"tiny": StrategyProfile()},
        markets=[MarketEntry(slug=meta.slug, profile="tiny")],
    )
    snapshot = await collect_live_snapshot(
        cfg,
        gateway=EmptyGateway(),
        service_status={
            "load_state": "not-found", "active_state": "inactive", "sub_state": "dead",
            "main_pid": 0, "restarts": 0, "active_since": "",
        },
    )
    assert snapshot["summary"]["open_order_count"] == 0
    assert snapshot["positions"] == []
    assert snapshot["markets"][0]["regime"] == "STOPPED"
    assert snapshot["summary"]["ledger_matches_positions"]


def test_control_panel_assets_do_not_embed_secrets(tmp_path):
    app = ControlPanelApplication(BitviseControl(_settings(tmp_path)), "127.0.0.1", 8765, 20)
    page = app.page().decode()
    script, content_type = app.asset("app.js")
    assert app.control_token in page
    assert "__CONTROL_TOKEN__" not in page
    assert "PK=" not in page
    assert b"innerHTML" not in script
    assert content_type.startswith("text/javascript")
    assert app.valid_host("127.0.0.1:8765")
    assert app.valid_origin("http://127.0.0.1:8765")
    assert not app.valid_origin("https://attacker.example")


def test_control_panel_refuses_non_loopback_bind(tmp_path):
    with pytest.raises(ValueError, match="loopback"):
        serve_control_panel(_settings(tmp_path), host="0.0.0.0", open_browser=False)


def test_http_control_endpoint_requires_same_origin_and_random_token(tmp_path):
    remote = FakeRemote()
    app = ControlPanelApplication(remote, "127.0.0.1", 0, 20)  # type: ignore[arg-type]
    server = _ControlServer(("127.0.0.1", 0), ControlRequestHandler, app)
    port = int(server.server_address[1])
    app.port = port
    app.origins = {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        denied = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        denied.request(
            "POST",
            "/api/action",
            body='{"action":"stop"}',
            headers={"Content-Type": "application/json", "Origin": "https://attacker.example"},
        )
        assert denied.getresponse().status == 403
        denied.close()
        assert remote.actions == []

        allowed = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        allowed.request(
            "POST",
            "/api/action",
            body='{"action":"restart"}',
            headers={
                "Content-Type": "application/json",
                "Origin": f"http://127.0.0.1:{port}",
                "X-Control-Token": app.control_token,
            },
        )
        response = allowed.getresponse()
        payload = json.loads(response.read())
        allowed.close()
        assert response.status == 200
        assert payload["result"]["action"] == "restart"
        assert remote.actions == ["restart"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
