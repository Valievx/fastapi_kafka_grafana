DC = docker compose
EXEC = docker exec -it
LOGS = docker logs
ENV_FILE = .env
APP_FILE = docker-compose.yaml


.PHONY: app
app:
	${DC} --env-file ${ENV_FILE} -f ${APP_FILE} up --build -d
