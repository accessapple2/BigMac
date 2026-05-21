"""HM-WR-CANCEL-ON-TIMEOUT 2026-05-21 — OllamaProvider cancel-on-timeout regression suite.

Bug surface: war_room._WR_PROVIDER_TIMEOUT_S=90 outer budget did NOT cancel the
inner `requests.post(... ollama)` call. The HTTP call kept running until
self.timeout (180s default), holding the OllamaQueue worker and starving every
subsequent agent in the same WR cycle.

Fix: default timeout cut to _HM_WR_CANCEL_BUDGET_S=85, slightly under WR's 90s,
so `requests.post` raises ReadTimeout first. urllib3 closes the socket on
ReadTimeout; Ollama detects the disconnect and cancels server-side. The wrapper
also logs `[OLLAMA-CANCEL]` and re-raises so the WR loop can move on cleanly.

See: project_hm_wr_ollama_queue_starvation.md
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests


def test_cancel_budget_is_below_wr_outer_budget() -> None:
    """Cancel budget must be strictly less than war_room's outer 90s timeout."""
    from engine.providers.ollama_provider import _HM_WR_CANCEL_BUDGET_S
    from engine.war_room import _WR_PROVIDER_TIMEOUT_S

    assert _HM_WR_CANCEL_BUDGET_S < _WR_PROVIDER_TIMEOUT_S, (
        f"cancel budget {_HM_WR_CANCEL_BUDGET_S}s must be < WR outer "
        f"timeout {_WR_PROVIDER_TIMEOUT_S}s so HTTP closes the socket "
        f"before WR's Future timeout fires"
    )


def test_default_timeout_matches_cancel_budget() -> None:
    """New OllamaProvider instances default to the cancel budget, not the pre-fix 180s."""
    from engine.providers.ollama_provider import OllamaProvider, _HM_WR_CANCEL_BUDGET_S

    p = OllamaProvider()
    assert p.timeout == _HM_WR_CANCEL_BUDGET_S


def test_agent_routing_default_timeout_matches_cancel_budget() -> None:
    """build_provider + build_all_providers default to the cancel budget."""
    import inspect

    from engine import agent_routing

    sig_build = inspect.signature(agent_routing.build_provider)
    sig_all = inspect.signature(agent_routing.build_all_providers)
    assert sig_build.parameters["default_timeout"].default == 85
    assert sig_all.parameters["default_timeout"].default == 85


class _StubQueue:
    """Pass-through queue stub — calls fn() directly. The queue is not under test."""

    def submit(self, fn, model_id: str = ""):
        return fn()


def test_call_model_logs_cancel_on_read_timeout(caplog) -> None:
    """ReadTimeout → [OLLAMA-CANCEL] log + re-raise."""
    from engine.providers.ollama_provider import OllamaProvider

    p = OllamaProvider(player_id="test-agent", model="ministral-3:3b", timeout=1)

    with patch("engine.providers.ollama_provider.get_queue", return_value=_StubQueue()), \
         patch("engine.providers.ollama_provider.requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ReadTimeout("read timed out")
        with caplog.at_level(logging.WARNING, logger="ollama_provider"):
            with pytest.raises(requests.exceptions.ReadTimeout):
                p.call_model("test prompt")

    cancel_logs = [r for r in caplog.records if "[OLLAMA-CANCEL]" in r.getMessage()]
    assert cancel_logs, "expected [OLLAMA-CANCEL] log line on ReadTimeout"
    msg = cancel_logs[0].getMessage()
    assert "model=ministral-3:3b" in msg
    assert "agent=test-agent" in msg
    assert "reason=ReadTimeout" in msg


def test_call_model_logs_cancel_on_connection_error(caplog) -> None:
    """ConnectionError → [OLLAMA-CANCEL] log + re-raise. Same code path as
    ReadTimeout: covers the case where the Ollama socket dies mid-flight."""
    from engine.providers.ollama_provider import OllamaProvider

    p = OllamaProvider(player_id="test-agent", model="ministral-3:3b", timeout=1)

    with patch("engine.providers.ollama_provider.get_queue", return_value=_StubQueue()), \
         patch("engine.providers.ollama_provider.requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ConnectionError("connection lost")
        with caplog.at_level(logging.WARNING, logger="ollama_provider"):
            with pytest.raises(requests.exceptions.ConnectionError):
                p.call_model("test prompt")

    cancel_logs = [r for r in caplog.records if "[OLLAMA-CANCEL]" in r.getMessage()]
    assert cancel_logs, "expected [OLLAMA-CANCEL] log line on ConnectionError"
    assert "reason=ConnectionError" in cancel_logs[0].getMessage()


def test_call_model_no_cancel_log_on_normal_response(caplog) -> None:
    """Normal response → no [OLLAMA-CANCEL] log. Sanity check."""
    from engine.providers.ollama_provider import OllamaProvider

    p = OllamaProvider(player_id="test-agent", model="ministral-3:3b", timeout=85)
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"response": "hello world"}

    with patch("engine.providers.ollama_provider.get_queue", return_value=_StubQueue()), \
         patch("engine.providers.ollama_provider.requests.post", return_value=mock_resp):
        with caplog.at_level(logging.WARNING, logger="ollama_provider"):
            result = p.call_model("test prompt")

    assert result == "hello world"
    cancel_logs = [r for r in caplog.records if "[OLLAMA-CANCEL]" in r.getMessage()]
    assert not cancel_logs, "no [OLLAMA-CANCEL] expected on a clean call"


def test_requests_post_receives_self_timeout() -> None:
    """The timeout we pass to requests.post matches self.timeout — not a hard-coded value."""
    from engine.providers.ollama_provider import OllamaProvider

    p = OllamaProvider(player_id="test-agent", model="ministral-3:3b", timeout=42)
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"response": "ok"}

    with patch("engine.providers.ollama_provider.get_queue", return_value=_StubQueue()), \
         patch("engine.providers.ollama_provider.requests.post", return_value=mock_resp) as mock_post:
        p.call_model("prompt")

    assert mock_post.call_args.kwargs["timeout"] == 42
