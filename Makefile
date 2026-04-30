COMPOSE_FILE := docker-compose.dev.yml
DOCKER_COMPOSE := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo ""; fi)

.PHONY: dev-up dev-down dev-reset dev-logs dev-ps dev-health

dev-up:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "docker compose/docker-compose not found"; exit 1; fi
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --build
	$(MAKE) dev-health

dev-down:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "docker compose/docker-compose not found"; exit 1; fi
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down

dev-reset:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "docker compose/docker-compose not found"; exit 1; fi
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v

dev-logs:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "docker compose/docker-compose not found"; exit 1; fi
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

dev-ps:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "docker compose/docker-compose not found"; exit 1; fi
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) ps

dev-health:
	@echo "Waiting for declaration-service health endpoint..."
	@for i in $$(seq 1 30); do \
		if curl -fsS http://localhost:8001/health >/dev/null; then \
			echo "declaration-service is healthy"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "declaration-service did not become healthy in time"; \
	exit 1
