.PHONY: server
server:
	uvicorn dagops.server:app --host 0.0.0.0 --port 5002 --reload
