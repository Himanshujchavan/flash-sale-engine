.PHONY: setup test test-cov lint format dev-up dev-down lint-fix

setup:		## Setup development environment
	python -m venv venv
	.\venv\Scripts\activate
	pip install -r requirements-dev.txt

test:		## Run test suite
	pytest --cov=shared --cov-report=term-missing

test-cov:	## Run tests with detailed coverage
	pytest --cov=shared --cov-report=html

lint:		## Run linting
	flake8 shared/ order_service/ inventory_service/ payment_service/ saga_coordinator/ notification_service/

format:	## Format code
	black shared/ order_service/ inventory_service/ payment_service/ saga_coordinator/ notification_service/
	isort shared/ order_service/ inventory_service/ payment_service/ saga_coordinator/ notification_service/

dev-up:		## Start all services for development
	docker compose up -d

dev-down:		## Stop development services
	docker compose down

lint-fix:	## Auto-fix linting issues
	autopep8 --in-place --recursive shared/ order_service/ inventory_service/ payment_service/ saga_coordinator/ notification_service/