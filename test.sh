#!/usr/bin/env bash
. .test_env
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

make db-schema-d

pytest -p no:warnings

make deps-down
