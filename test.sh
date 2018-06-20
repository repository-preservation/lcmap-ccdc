#!/usr/bin/env bash
. .test_env
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
pytest -p no:warnings
