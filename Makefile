
.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "install"
	@echo "fmt"
	@echo "check-types"

.PHONY: install
install:
	pip install --upgrade pip
	pip install -e .

.PHONY: install-dev
install-dev:
	pip install --upgrade pip
	pip install -e .[dev]

.PHONY: fmt
fmt:
	ruff format .
	ruff check --fix . 

.PHONY: check-types
check-types:
	basedpyright

