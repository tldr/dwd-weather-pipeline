#!/bin/bash
set -euo pipefail

# Activate virtual environment
source .venv/bin/activate

echo "Running code quality checks..."

echo "1/4 Running black..."
black --config pyproject.toml --check src/ tests/

echo "2/4 Running isort..."
isort --settings-path pyproject.toml --check-only src/ tests/

echo "3/4 Running ruff..."
ruff check --config pyproject.toml src/ tests/

echo "4/4 Running mypy..."
echo "Starting dmypy server..."
dmypy start
echo "Running mypy checks..."
dmypy run src/

echo "Stopping dmypy server..."
dmypy stop

echo "5/5 Running pytest..."
pytest

echo "All checks completed!"
