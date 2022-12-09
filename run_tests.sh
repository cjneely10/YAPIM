#!/usr/bin/env bash
### Run YAPIM test suite
### Installation:
### - pip install .
### Usage: ./run_tests.sh

# Display commands and exit on non-zero return status
set -xe

# Top-level test directory
TESTS=tests
# Test directories
TEST_DIRECTORIES=(cli config_manager dependency_graph executor)

cd "$TESTS" || exit 1
for test_dir in "${TEST_DIRECTORIES[@]}"; do
  cd "$test_dir" || exit 1
  # Run test_*.py scripts, exit on failure
  pytest . --cov=yapim
  # Delete temporary test directories that may have been created
  rm -rf ./*-out
  cd - || exit 1
done

cd .. || exit 1

# Lint code
pylint yapim
