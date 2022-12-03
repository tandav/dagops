.PHONY: server
server:
	uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload

.PHONY: bumpver
bumpver:
	# usage: make bumpver PART=minor
	bumpver update --no-fetch --$(PART)
