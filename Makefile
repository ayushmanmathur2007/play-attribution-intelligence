.PHONY: setup generate validate eval demo serve test clean

PYTHON = .venv/bin/python
STREAMLIT = .venv/bin/streamlit

setup:
	/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	mkdir -p data/synthetic data/eval/traces traces

generate:
	$(PYTHON) scripts/generate_data.py --output data/synthetic/ --months 18
	$(PYTHON) -m src.data_generator.validate data/synthetic/

validate:
	$(PYTHON) -m src.data_generator.validate data/synthetic/ --plots

eval:
	$(PYTHON) scripts/run_eval.py \
		--config config/local.yaml \
		--golden data/synthetic/metric_movements_golden.csv \
		--output data/eval/

demo:
	$(PYTHON) scripts/demo.py --query "$(QUERY)" --config config/local.yaml

demo-easy:
	$(PYTHON) scripts/demo.py --example 1

demo-hard:
	$(PYTHON) scripts/demo.py --example 3

serve:
	$(STREAMLIT) run src/ui/app.py -- --data-dir data/synthetic/ --eval-dir data/eval/

test:
	$(PYTHON) -m pytest tests/ -v

clean:
	rm -rf data/synthetic/* data/eval/* traces/*

# GCP commands
gcp-upload:
	gsutil cp -r data/synthetic/ gs://$(GCP_BUCKET)/play-attribution/synthetic/

gcp-build:
	docker build -t gcr.io/$(GCP_PROJECT)/play-attribution .
	docker push gcr.io/$(GCP_PROJECT)/play-attribution

gcp-deploy:
	bash scripts/deploy_gcp.sh
