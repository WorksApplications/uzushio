#!/bin/bash

#$ -j y
#$ -cwd
#$ -l USE_SSH=1
#$ -l USE_EXTRA_NETWORK=1

du -hs "$1" > /dev/null &

# SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
UZUSHIO_ROOT=$HOME/work/uzushio
SCRIPT_DIR=$UZUSHIO_ROOT/scripts
export SPARK_HOME=${SPARK:-$HOME/soft/spark-3.4.1-bin-hadoop3}
UZUSHIO_JAR=$(readlink -f "$SCRIPT_DIR/../core/target/scala-2.12/uzushio-assembly-0.1.0-SNAPSHOT.jar")

export SPARK_CONF_DIR=$UZUSHIO_ROOT/spark-config/abci-f
"$SPARK_HOME/sbin/start-master.sh"
export SPARK_WORKERS=$SGE_JOB_HOSTLIST
export SPARK_SSH_OPTS="-p 2222"
export SPARK_LOCAL_DIRS=$SGE_LOCALDIR
SPARK_MASTER="spark://$(hostname):7077"
"$SPARK_HOME/sbin/workers.sh" "SPARK_CONF_DIR=$UZUSHIO_ROOT/spark-config/abci-f" "$SPARK_HOME/sbin/start-worker.sh" $SPARK_MASTER

# it is possible to monitor task progress with Spark UI accessible by ssh port forwarding
echo "$(date -Iseconds) $JOB_ID ssh abci -L8080:$(hostname):8080" >> /scratch/$USER/spark-ui-monitoring

mkdir -p /scratch/$USER/spark-exlog

INPUT=$1
OUTPUT=$2
CACHE=/dev/null
NUM_PARTITIONS=${3:-50}
NUM_PARTITIONS_PROPAGATION=${4:-$(($NUM_PARTITIONS * 4))}

"$SPARK_HOME/bin/spark-submit" \
    --class com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master $SPARK_MASTER \
    --conf spark.driver.log.dfsDir=/scratch/$USER/spark-exlog \
    --conf spark.eventLog.dir=/scratch/$USER/spark-exlog \
    --conf spark.local.dir=$SPARK_LOCAL_DIRS \
    --conf spark.sql.shuffle.partitions=${NUM_PARTITIONS_PROPAGATION} \
    --conf spark.sql.parquet.columnarReaderBatchSize=512 \
    local://$UZUSHIO_JAR \
    --input="$INPUT" \
    --output="$OUTPUT" \
    --execution=reprHashes,stats,saveStats \
    --propagate-partitions=$NUM_PARTITIONS_PROPAGATION \
    --partitions=$NUM_PARTITIONS --intermediate

wait