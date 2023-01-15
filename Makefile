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

.PHONY: create_db
create_db:
	python -m dagops.state.database create
