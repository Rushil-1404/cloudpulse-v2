.PHONY: install test seed serve dashboard demo

install:
	pip install -e ".[all,dev]"

test:
	python -m pytest -q

seed:
	cloudpulse seed --days 7 --per-day 300

serve:
	cloudpulse serve

dashboard:
	cloudpulse dashboard

# Local demo without AWS: CLOUDPULSE_AUTH_DISABLED=true is for development only.
demo: seed
	CLOUDPULSE_AUTH_DISABLED=true cloudpulse serve
