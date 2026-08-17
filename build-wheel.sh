#!/bin/bash
# Build the ETL Framework wheel and copy to infra assets directory
set -e

echo "Building ETL Framework wheel..."

cd etl-framework

# Clean previous builds
rm -rf build/ dist/ *.egg-info

# Build wheel
python3 setup.py bdist_wheel

# Copy wheel to infra assets
mkdir -p ../infra/assets/wheels
cp dist/*.whl ../infra/assets/wheels/

echo "Wheel built successfully:"
ls -la ../infra/assets/wheels/*.whl

cd ..
