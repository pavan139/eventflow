PYTHON ?= python
.PHONY: install test demo evaluate
install:
	$(PYTHON) -m pip install -e .
test:
	$(PYTHON) -m unittest discover -s recovery_tests -v
demo:
	$(PYTHON) -m eventflow demo --output results/demo.json
evaluate:
	$(PYTHON) -m eventflow evaluate --output results/evaluation.json
