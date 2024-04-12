docker-spin-up:
	docker compose --env-file env up --build -d

grant:
	sudo chmod -R u=rwx,g=rwx,o=rwx containers logs scheduler scripts streamlit test

sleeper:
	sleep 15

up: docker-spin-up sleeper 

down: 
	docker compose --env-file env down

shell:
	docker exec -ti pipelinerunner bash

format:
	docker exec pipelinerunner python -m black -S --line-length 79 .

isort:
	docker exec pipelinerunner isort .

pytest:
	docker exec pipelinerunner pytest /code/test

type:
	docker exec pipelinerunner mypy --ignore-missing-imports /code

lint: 
	docker exec pipelinerunner flake8 /code 

ci: isort format type lint pytest

start-etl:
	docker exec pipelinerunner service cron start

status-etl:

	docker exec pipelinerunner service cron status

stop-etl: 
	docker exec pipelinerunner service cron stop
