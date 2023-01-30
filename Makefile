include .env
export

.PHONY: server
server:
	uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload

.PHONY: daemon
daemon:
	python examples/main.py

.PHONY: bumpver
bumpver:
	# usage: make bumpver PART=minor
	bumpver update --no-fetch --$(PART)

.PHONY: create
create:
	python -m dagops.state.database create

.PHONY: drop
drop:
	python -m dagops.state.database drop

.PHONY: workers
workers:
	python -m dagops.worker
