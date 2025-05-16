#!/bin/bash
set -euo pipefail

# Activate virtual environment
source .venv/bin/activate

echo "Running code quality checks..."

echo "1/4 Running black..."
black --config pyproject.toml --check src/

echo "2/4 Running isort..."
isort --settings-path pyproject.toml --check-only src/

echo "3/4 Running ruff..."
ruff check src/

echo "4/4 Running mypy..."
dmypy run src/

echo "All checks completed!"
