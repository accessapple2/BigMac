#!/bin/bash
echo "RED ALERT — shutting down OllieTrades fleet..."
pkill -f main.py
pkill -f etf_regime
pkill -f morning_briefing
pkill -f options_flow
pkill -f tractor_beam
launchctl stop com.ollietrades.morningbriefing
launchctl stop com.ollietrades.etfregime
launchctl stop com.ollietrades.optionsflow
launchctl stop com.ollietrades.ghosttrader
echo "All systems offline. Shields down."
