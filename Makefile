.PHONY: server
server:
	# nohup uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload
	uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload

.PHONY: daemon
daemon:
	python main.py

.PHONY: bumpver
bumpver:
	# usage: make bumpver PART=minor
	bumpver update --no-fetch --$(PART)

.PHONY: create_db
create_db:
	python -m dagops.state.database create
