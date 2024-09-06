####################################################################################################################
# Setup containers to run Airflow
NETWORK_NAME=data-network

install:
	@echo "Run install docker"
	@chmod +x ./scripts/install_docker.sh
	@./scripts/install_docker.sh
	@echo "Run install docker complete"

init:
	@echo "Run init project"
	@chmod +x ./scripts/init_project.sh
	@./scripts/init_project.sh $(NETWORK_NAME)
	@echo "Run init project complete"

depend: mongodb nginx

up:
	@echo "Spin up Airflow"
	@cd ./airflow && docker compose up -d --build && cd ..
	@echo "Spin up Airflow complete"

mongodb:
	@echo "Setup mongodb"
	@cd ./mongodb && docker compose -f docker-compose.yaml up -d --build && cd ..
	@echo "Setup domain complete"

nginx:
	@echo "Setup nginx"
	@cd ./nginx && docker compose -f docker-compose.yaml up -d --build && cd ..
	@echo "Setup domain complete"

## Airflow Webserver

ui:
	open http://localhost:8080

down:
	@echo "Shutdown Airflow"
	@cd ./airflow && docker compose down && cd ..
	@echo "Shutdown Airflow complete"

deploy: init depend up

restart: down up