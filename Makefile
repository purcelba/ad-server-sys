.PHONY: up down test demo eda reach features replay consumer serve-features loadtest-features train eval-plots serve-ads loadtest-serve serve-bidder reconcile retrain readout

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

reach: demo
	uv run python -m adserver.batch_features.reach

features: demo
	uv run python -m adserver.batch_features.cli

consumer: demo
	uv run python -m adserver.stream_features.consumer

replay: demo
	uv run python -m adserver.datagen.replay

serve-features: features
	uv run python -m adserver.feature_service.service

loadtest-features:
	uv run python -m adserver.feature_service.loadtest

train: demo
	uv run python -m adserver.ranking.train --version v1

eval-plots:
	uv run python -m adserver.ranking.eval_plots

serve-bidder:
	uv run python -m adserver.bidder_stub.service

serve-ads: features
	uv run python -m adserver.adserver.service

loadtest-serve:
	uv run python -m adserver.adserver.loadtest

reconcile:
	uv run python -m adserver.ops.reconcile

retrain:
	uv run python -m adserver.ranking.retrain
