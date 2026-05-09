#!/bin/sh

set -e
mkdir -p /tmp/dbt-project && cd /tmp/dbt-project
mkdir -p models/silver models/gold tests macros
cp /tmp/cm/dbt_project.yml .
cp /tmp/cm/profiles.yml .
cp /tmp/cm/sources.yml models/sources.yml
for f in /tmp/cm/macros_*.sql; do
  bn=$(basename "$f" | sed 's/^macros_//')
  cp "$f" "macros/$bn"
done
for f in /tmp/cm/silver_*.sql; do
  bn=$(basename "$f" | sed 's/^silver_//')
  cp "$f" "models/silver/$bn"
done
cp /tmp/cm/silver__silver.yml models/silver/_silver.yml
for f in /tmp/cm/gold_*.sql; do
  bn=$(basename "$f" | sed 's/^gold_//')
  cp "$f" "models/gold/$bn"
done
cp /tmp/cm/gold__gold.yml models/gold/_gold.yml
for f in /tmp/cm/test_*.sql; do
  bn=$(basename "$f" | sed 's/^test_//')
  cp "$f" "tests/$bn"
done
