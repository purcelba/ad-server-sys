.PHONY: up down test demo eda

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@for i in $$(seq 1 60); do \
		healthy=$$(docker compose ps --format json | grep -o '"Health":"healthy"' | wc -l | tr -d ' '); \
		if [ "$$healthy" = "3" ]; then echo "All infra healthy."; exit 0; fi; \
		sleep 2; \
	done; \
	echo "Timed out waiting for infra to become healthy."; \
	docker compose ps; \
	exit 1

down:
	docker compose down -v

test:
	uv run pytest adserver/ -v

demo:
	uv run python -m adserver.datagen.cli --seed 42 --out data/
	@echo "Generated users.parquet, campaigns.parquet, events.parquet, rides.parquet in data/"
	uv run python -c "import polars as pl; [print(f, '\n', pl.read_parquet(f'data/{f}').head(), '\n') for f in ['users.parquet', 'campaigns.parquet', 'events.parquet', 'rides.parquet']]"

eda: demo
	uv run python -m adserver.datagen.eda --data-dir data/ --out data/eda/
	@echo "Wrote EDA plots to data/eda/"
