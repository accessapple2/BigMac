# OllieTrades — operator make targets.
PY := .venv/bin/python3

.PHONY: help status status-json

help:  ## List targets
	@grep -hE '^[a-z][a-zA-Z0-9_-]*:.*##' $(MAKEFILE_LIST) | \
	  sed 's/:.*##/\t/' | sort

status:  ## One-command fleet status (gates/regime/fleet/git/services/briefings — read-only, no server)
	@$(PY) -m engine.fleet_status

status-json:  ## Fleet status as raw JSON
	@$(PY) -m engine.fleet_status --json
