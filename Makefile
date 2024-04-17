####################################################################################################################
# Spin up docker container

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

####################################################################################################################
# Set up cloud infrastructure

tf-init:
	terraform -chdir=./terraform init

infra-up:
	terraform -chdir=./terraform apply

infra-down:
	terraform -chdir=./terraform destroy

infra-config:
	terraform -chdir=./terraform output

####################################################################################################################
# Helpers

ssh-ec2:
	terraform -chdir=./terraform output -raw private_key > private_key.pem && chmod 600 private_key.pem && ssh -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i private_key.pem ubuntu@$$(terraform -chdir=./terraform output -raw ec2_public_dns) && rm private_key.pem