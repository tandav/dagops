.PHONY: server
server:
	# nohup uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload
	uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload

.PHONY: daemon
daemon:
	python -m dagops.daemon

.PHONY: bumpver
bumpver:
	# usage: make bumpver PART=minor
	bumpver update --no-fetch --$(PART)
