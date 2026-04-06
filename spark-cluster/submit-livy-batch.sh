#!/usr/bin/env bash
set -euo pipefail
# Submit SparkCompareJob to local Livy. Run from repo root after mvn package and docker compose up.

URI="http://localhost:8998/batches"
curl -sS -X POST "$URI" \
  -H "Content-Type: application/json" \
  -d '{
  "file": "file:///data/cmdb-compare-service-0.0.1-SNAPSHOT-spark-job.jar",
  "className": "com.cmdb.compare.job.SparkCompareJob",
  "args": [
    "file:///data/source.csv",
    "file:///data/target.csv",
    "id",
    "null",
    "null",
    "",
    "file:///data/out/",
    "http://127.0.0.1:9000",
    "dummy",
    "dummy"
  ],
  "conf": {
    "spark.master": "local[*]",
    "spark.submit.deployMode": "client",
    "spark.yarn.submit.waitAppCompletion": "false"
  }
}' | tee /dev/stderr

echo ""
