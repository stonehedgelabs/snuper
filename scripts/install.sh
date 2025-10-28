#!/bin/sh

python -m venv venv
./venv/bin/pip install --upgrade poetry
./venv/bin/poetry install
./venv/bin/playwright install
./venv/bin/playwright install-deps