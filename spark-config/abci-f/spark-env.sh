#!/usr/bin/env bash

# Spark environment variables for ABCI rt_F nodes
# Correctly set temp directory and local storage to local scratch directory

export SPARK_LOCAL_DIRS="$SGE_LOCALDIR"
export SPARK_DAEMON_JAVA_OPTS="-Djava.io.tmpdir=$SGE_LOCALDIR"

export SPARK_WORKER_DIR="$SGE_LOCALDIR"
export SPARK_WORKER_OPTS="-Djava.io.tmpdir=$SGE_LOCALDIR"
export SPARK_EXECUTOR_OPTS="-Djava.io.tmpdir=$SGE_LOCALDIR"

export SPARK_LOG_DIR="/scratch/${USER:-nouser}/spark-log/${JOB_ID:-nojob}"
mkdir -p "$SPARK_LOG_DIR"

export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
