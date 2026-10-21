export PYTHONPATH=src

.PHONY: check-env setup mock mock-tiny mock-medium silver gold hive-local mr-local \
        spark-count pagerank kmeans analytics daily-report merkle \
        contracts-build contracts-test chain-local \
        stream-mock stream-replay figures summary dashboard \
        test test-slow e2e-local \
        package gcp-bootstrap gcp-cluster gcp-upload gcp-check-range gcp-estimate \
        gcp-export gcp-silver gcp-gold gcp-hive gcp-mr gcp-bench gcp-pagerank \
        gcp-kmeans gcp-analytics gcp-report gcp-stream gcp-pull gcp-delete gcp-batch \
        sepolia-check sepolia-deploy sepolia-publish sepolia-verify clean

# ---- Environment ----
check-env:
	bash scripts/check_env.sh

setup: check-env

# ---- Mock data ----
mock:
	python -m etherlens.mock.generate_batch --preset small --seed 42 \
	  --start-date 2025-01-01 --out data/bronze/tx

mock-tiny:
	python -m etherlens.mock.generate_batch --preset tiny --seed 42 \
	  --start-date 2025-01-01 --out data/bronze/tx

mock-medium:
	python -m etherlens.mock.generate_batch --preset medium --seed 42 \
	  --start-date 2025-01-01 --out data/bronze/tx

# ---- Batch pipeline ----
silver:
	python jobs/silver_main.py --env local

gold:
	python jobs/gold_main.py --env local

hive-local:
	python scripts/run_hive_local.py

mr-local:
	bash mapreduce/run_local.sh

spark-count:
	python jobs/count_spark_main.py --env local

pagerank:
	python jobs/pagerank_main.py --env local

kmeans:
	python jobs/kmeans_main.py --env local

analytics:
	python jobs/analytics_main.py --env local

daily-report:
	python jobs/daily_report_main.py --env local

merkle:
	python -m etherlens.chain.merkle build --all --env local

# ---- Contracts ----
contracts-build:
	cd contracts && forge build

contracts-test:
	cd contracts && forge test -vv

chain-local:
	$(MAKE) contracts-build
	python -m etherlens.chain.deploy
	python -m etherlens.chain.publish --all
	pytest tests/test_chain_local.py -v -m chain

# ---- Streaming ----
stream-mock:
	bash scripts/stream_local.sh mock

stream-replay:
	bash scripts/stream_local.sh replay

# ---- Results ----
figures:
	python scripts/make_figures.py --root results --gold data/gold

summary:
	python scripts/make_summary.py

dashboard:
	streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501

# ---- Tests ----
test:
	pytest tests/ -v --ignore=tests/test_stream_e2e.py --ignore=tests/test_chain_local.py \
	  -m "not slow and not chain"

test-slow:
	pytest tests/ -v -m slow

# ---- End-to-end local ----
e2e-local:
	$(MAKE) mock
	$(MAKE) silver
	$(MAKE) gold
	$(MAKE) hive-local
	$(MAKE) mr-local
	$(MAKE) spark-count
	$(MAKE) pagerank
	$(MAKE) kmeans
	$(MAKE) analytics
	$(MAKE) daily-report
	$(MAKE) merkle
	$(MAKE) chain-local
	$(MAKE) figures
	$(MAKE) summary
	$(MAKE) test

# ---- Cloud packaging ----
package:
	mkdir -p dist
	find src -name __pycache__ -type d -prune -exec rm -rf {} +
	python -c "import shutil; shutil.make_archive('dist/etherlens', 'zip', 'src', 'etherlens')"

# ---- GCP bootstrap ----
gcp-bootstrap:
	bash infra/gcp_bootstrap.sh

gcp-cluster:
	bash infra/create_cluster.sh

gcp-upload:
	bash infra/upload_code.sh

gcp-check-range:
	python scripts/render_template.py sql/check_range.sql \
	  | bq query --use_legacy_sql=false --location=US

gcp-estimate:
	python scripts/render_template.py sql/estimate_bronze.sql \
	  | bq query --use_legacy_sql=false --location=US --dry_run

gcp-export:
	python scripts/render_template.py sql/export_bronze.sql \
	  | bq query --use_legacy_sql=false --location=US

# ---- GCP jobs ----
gcp-batch:
	bash scripts/run_gcp_batch.sh

gcp-silver:
	bash infra/submit.sh silver --env gcp --bucket $(GCS_BUCKET)

gcp-gold:
	bash infra/submit.sh gold --env gcp --bucket $(GCS_BUCKET)

gcp-hive:
	gcloud dataproc jobs submit hive \
	  --cluster=$(CLUSTER_NAME) --region=$(GCP_REGION) \
	  --file=gs://$(GCS_BUCKET)/code/hive/create_tables.hql
	mkdir -p logs
	gcloud dataproc jobs submit hive \
	  --cluster=$(CLUSTER_NAME) --region=$(GCP_REGION) \
	  --file=gs://$(GCS_BUCKET)/code/hive/queries.hql \
	  2>&1 | tee logs/hive_queries_$$(date +%Y%m%d_%H%M%S).log

gcp-mr:
	bash mapreduce/run_dataproc.sh

gcp-bench:
	bash scripts/bench_mr_vs_spark.sh

gcp-pagerank:
	bash infra/submit.sh pagerank --env gcp --bucket $(GCS_BUCKET)

gcp-kmeans:
	bash infra/submit.sh kmeans --env gcp --bucket $(GCS_BUCKET)

gcp-analytics:
	bash infra/submit.sh analytics --env gcp --bucket $(GCS_BUCKET)

gcp-report:
	bash infra/submit.sh daily_report --env gcp --bucket $(GCS_BUCKET)

gcp-stream:
	bash infra/submit.sh stream --env gcp --bucket $(GCS_BUCKET)

gcp-pull:
	gcloud storage rsync -r gs://$(GCS_BUCKET)/results results_gcp/

gcp-delete:
	bash infra/delete_cluster.sh

# ---- Sepolia ----
sepolia-check:
	CHAIN=sepolia python -c "from etherlens.chain.common import get_w3, get_account; \
	w3 = get_w3(); a = get_account(w3); \
	print('chain id:', w3.eth.chain_id, '(Sepolia = 11155111)'); \
	print('deployer:', a.address); \
	print('balance :', w3.from_wei(w3.eth.get_balance(a.address), 'ether'), 'SepoliaETH')"

sepolia-deploy:
	CHAIN=sepolia python -m etherlens.chain.deploy --results results_gcp

sepolia-publish:
	CHAIN=sepolia python -m etherlens.chain.publish --last 3 --results results_gcp

sepolia-verify:
	@echo "Usage: make sepolia-verify DT=2025-01-31 IDX=0 [TAMPER=--tamper]"
	CHAIN=sepolia python -m etherlens.chain.verify --dt $(DT) --index $(IDX) $(TAMPER) --results results_gcp

# ---- Clean ----
clean:
	rm -rf data/ results/figures/* results/stream results/hive \
	  results/silver_stats.json results/gold_stats.json \
	  results/analytics_types.json results/summary.md \
	  results/mapreduce results/pagerank_top.csv results/pagerank_iters.json \
	  results/kmeans_* results/merkle results/chain \
	  results/spark_count_timing.json results/bench_mr_vs_spark.json \
	  logs/ build/ dist/
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
