#!/bin/bash

#$ -j y
#$ -cwd
#$ -l USE_SSH=1

# SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
UZUSHIO_ROOT=$HOME/work/uzushio
SCRIPT_DIR=$UZUSHIO_ROOT/scripts
export SPARK_HOME=${SPARK:-$HOME/soft/spark-3.3.2-bin-hadoop3}
UZUSHIO_JAR=$(readlink -f "$SCRIPT_DIR/../target/scala-2.12/uzushio-assembly-0.1-SNAPSHOT.jar")

export SPARK_CONF_DIR=$UZUSHIO_ROOT/spark-config/abci-f
"$SPARK_HOME/sbin/start-master.sh"
export SPARK_WORKERS=$SGE_JOB_HOSTLIST
export SPARK_SSH_OPTS="-p 2222"
export SPARK_LOCAL_DIRS=$SGE_LOCALDIR
SPARK_MASTER="spark://$(hostname):7077"
"$SPARK_HOME/sbin/workers.sh" "SPARK_CONF_DIR=$UZUSHIO_ROOT/spark-config/abci-f" "$SPARK_HOME/sbin/start-worker.sh" $SPARK_MASTER

echo "$(date -Iseconds) $JOB_ID ssh abci -L8080:$(hostname):8080" >> /scratch/$USER/spark-ui-monitoring

mkdir -p /scratch/$USER/spark-exlog

"$SPARK_HOME/bin/spark-submit" \
    --class com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master $SPARK_MASTER \
    --conf spark.driver.log.dfsDir=/scratch/$USER/spark-exlog \
    --conf spark.eventLog.dir=/scratch/$USER/spark-exlog \
    --conf spark.local.dir=$SPARK_LOCAL_DIRS \
    $UZUSHIO_JAR \
    --input=$1 \
    --output=$2 \
    --cache=$3 \
    --execution=reprHashes,stats,saveStats \
    --propagate-partitions=400 \
    --partitions=200


# python3.9 $SCRIPT_DIR/spark_on_abci.py \
#     --jar=$UZUSHIO_JAR --spark=$SPARK \
#     --class=com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
#     --ping=$HOME/cc/pings \
#     --input=$1 \
#     --output=$2 \
#     --cache=$3 \
#     --execution=reprHashes,stats,saveStats \
#     --propagate-partitions=200 \
#     --partitions=200

