"""HM-TUNING-CREW-REPAIR-2026-07-06 tests for engine.gemini_free_tier's
_ollama_fallback() -- same silent-failure disease as
engine.crew.weekly_tuning_crew._ollama() (see tests/test_weekly_tuning_crew_wiring.py's
OllamaLoudFailureTests), fixed the same way: log loudly on a non-2xx
response or an empty response body instead of silently returning "".
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

import engine.gemini_free_tier as gft


class OllamaFallbackLoudFailureTests(unittest.TestCase):
    @patch("requests.post")
    def test_non_ok_response_logs(self, mock_post):
        mock_post.return_value.ok = False
        mock_post.return_value.status_code = 503
        mock_post.return_value.text = "Service Unavailable"
        with patch.object(gft.console, "log") as mock_log:
            result = gft._ollama_fallback("test prompt")
        self.assertEqual(result, "")
        logged = " ".join(str(c) for c in mock_log.call_args_list)
        self.assertIn("fallback failed", logged)
        self.assertIn("503", logged)

    @patch("requests.post")
    def test_empty_response_body_logs(self, mock_post):
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"response": ""}
        with patch.object(gft.console, "log") as mock_log:
            result = gft._ollama_fallback("test prompt")
        self.assertEqual(result, "")
        logged = " ".join(str(c) for c in mock_log.call_args_list)
        self.assertIn("empty response", logged)

    @patch("requests.post")
    def test_normal_response_does_not_log(self, mock_post):
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"response": "real content"}
        with patch.object(gft.console, "log") as mock_log:
            result = gft._ollama_fallback("test prompt")
        self.assertEqual(result, "real content")
        mock_log.assert_not_called()


if __name__ == "__main__":
    unittest.main()
