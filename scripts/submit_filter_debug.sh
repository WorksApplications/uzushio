#!/bin/bash

#$ -j y
#$ -cwd
#$ -l USE_SSH=1
#$ -l USE_EXTRA_NETWORK=1

INPUT=$1
STATS=$2
OUTPUT=$3

du -hs "$INPUT" > /dev/null &
du -hs "$STATS" > /dev/null &

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

NUM_PARTITIONS=1000
NUM_PARTITIONS_PROPAGATION=4000

"$SPARK_HOME/bin/spark-submit" \
    --class com.worksap.nlp.uzushio.main.DeduplicateParagraphs \
    --master $SPARK_MASTER \
    --conf spark.driver.log.dfsDir=/scratch/$USER/spark-exlog \
    --conf spark.eventLog.dir=/scratch/$USER/spark-exlog \
    --conf spark.local.dir=$SPARK_LOCAL_DIRS \
    --conf spark.sql.shuffle.partitions=$NUM_PARTITIONS_PROPAGATION \
    --conf spark.sql.parquet.columnarReaderBatchSize=256 \
    local://$UZUSHIO_JAR \
    --input=$INPUT \
    --cache=$STATS \
    --output=$OUTPUT \
    --propagate-partitions=$NUM_PARTITIONS_PROPAGATION \
    --filters=$SCRIPT_DIR/pipeline_01.conf \
    --partitions=$NUM_PARTITIONS \
    --execution=filter-debug \
    --format=json --compression=gzip --text-only


wait