"""Regression tests for the reward-aware and anti-sniping overlays."""

from __future__ import annotations

from polymaker.strategy.estimators import MidpointGuard


def test_midpoint_guard_filters_jump_and_requires_stability() -> None:
    guard = MidpointGuard()
    first = guard.update(
        0.50, 100.0, tick=0.001, jump_ticks=5, pause_s=3.0,
        stable_confirm_s=2.0, ema_alpha=0.35, median_window=5,
    )
    assert first.stable is False
    jumped = guard.update(
        0.52, 101.0, tick=0.001, jump_ticks=5, pause_s=3.0,
        stable_confirm_s=2.0, ema_alpha=0.35, median_window=5,
    )
    assert jumped.jumped is True
    assert jumped.paused is True
    assert jumped.stable is False

    still_paused = guard.update(
        0.52, 104.1, tick=0.001, jump_ticks=5, pause_s=3.0,
        stable_confirm_s=2.0, ema_alpha=0.35, median_window=5,
    )
    assert still_paused.paused is False
    assert still_paused.stable is False

    recovered = guard.update(
        0.52, 106.2, tick=0.001, jump_ticks=5, pause_s=3.0,
        stable_confirm_s=2.0, ema_alpha=0.35, median_window=5,
    )
    assert recovered.stable is True
    assert 0.50 < recovered.value < 0.52


def test_midpoint_guard_rejects_nonfinite_input() -> None:
    guard = MidpointGuard()
    try:
        guard.update(
            float("nan"), 1.0, tick=0.001, jump_ticks=5, pause_s=1.0,
            stable_confirm_s=1.0, ema_alpha=0.35, median_window=5,
        )
    except ValueError as exc:
        assert "finite" in str(exc)
    else:
        raise AssertionError("nonfinite midpoint must fail closed")
